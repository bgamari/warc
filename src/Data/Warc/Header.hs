{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Data.Warc.Header
    ( -- * Parsing
      header
      -- * Encoding
    , encodeHeader
      -- * WARC Version
    , Version(..)
    , warc0_16
      -- * Types
    , RecordHeader(..)
    , WarcType(..)
    , RecordId(..)
    , TruncationReason(..)
    , Digest(..)
    , Uri(..)
      -- * Header field types
    , Field(..)
    , lookupField
    , addField
    , mapField
      -- ** Standard fields
    , warcRecordId
    , contentLength
    , warcDate
    , warcType
    , contentType
    , warcConcurrentTo
    , warcBlockDigest
    , warcPayloadDigest
    , warcIpAddress
    , warcRefersTo
    , warcTargetUri
    , warcTruncated
    , warcWarcinfoID
    , warcFilename
    , warcProfile
    , warcSegmentNumber
    , warcSegmentTotalLength
      -- * Lenses
    , recWarcVersion, recHeaders
    ) where

import Control.Applicative
import Control.Monad (void)
import Data.Maybe (catMaybes)
import Data.Monoid ((<>))
import Data.Time.Clock
import Data.Time.Format
import Data.Char (ord)
import Data.String (IsString)

import Data.Attoparsec.ByteString.Char8 as A
import qualified Data.Attoparsec.ByteString.Lazy as AL
import qualified Data.HashMap.Strict as HM
import Data.Hashable (Hashable(..))
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Lazy as TL
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Builder as BB

import Control.Lens

withName :: String -> Parser a -> Parser a
withName name parser = parser <?> name

data Version = Version {versionMajor, versionMinor :: !Int}
             deriving (Show, Read, Eq, Ord)

warc0_16 :: Version
warc0_16 = Version 0 16

version :: Parser Version
version = withName "version" $ do
    "WARC/"
    major <- decimal
    char '.'
    minor <- decimal
    return (Version major minor)

newtype FieldName = FieldName {getFieldName :: Text}
                  deriving (Show, Read, IsString)

instance Hashable FieldName where
    hashWithSalt salt (FieldName t) = hashWithSalt salt (T.toCaseFold t)

instance Eq FieldName where
    FieldName a == FieldName b = T.toCaseFold a == T.toCaseFold b

instance Ord FieldName where
    FieldName a `compare` FieldName b = T.toCaseFold a `compare` T.toCaseFold b

separators :: String
separators = "()<>@,;:\\\"/[]?={}"

crlf :: Parser ()
crlf = void $ string "\r\n"

token :: Parser ByteString
token = takeTill (inClass $ separators++" \t\n\r")

utf8Token :: Parser Text
utf8Token = TE.decodeUtf8 <$> token

ord' = fromIntegral . ord

text :: Parser Text
text = do
    let content :: TL.Text -> Parser TL.Text
        content accum = do
            satisfy (isHorizontalSpace . ord')
            c <- takeTill (isEndOfLine . ord')
            continuation (accum <> TL.fromStrict (TE.decodeUtf8 c))
        continuation :: TL.Text -> Parser TL.Text
        continuation accum = content accum <|> return accum
    firstLine <- takeTill (isEndOfLine . ord')
    TL.toStrict <$> continuation (TL.fromStrict $ TE.decodeUtf8 firstLine)

quotedString :: Parser Text
quotedString = do
    char '"'
    c <- TE.decodeUtf8 <$> takeTill (== '"')
    char '"'
    return c

field :: Parser name -> Parser a -> Parser a
field name content = do
    try name
    char ':'
    skipSpace
    content <* endOfLine

data WarcType = WarcInfo
              | Response
              | Resource
              | Request
              | Metadata
              | Revisit
              | Conversion
              | Continuation
              | FutureType !Text
              deriving (Show, Read, Ord, Eq)

parseWarcType :: Parser WarcType
parseWarcType = choice
     [ "warcinfo"     *> pure WarcInfo
     , "response"     *> pure Response
     , "resource"     *> pure Resource
     , "request"      *> pure Request
     , "metadata"     *> pure Metadata
     , "revisit"      *> pure Revisit
     , "conversion"   *> pure Conversion
     , "continuation" *> pure Continuation
     , FutureType <$> utf8Token
     ]

encodeText :: T.Text -> BB.Builder
encodeText = BB.byteString . TE.encodeUtf8

encodeWarcType :: WarcType -> BB.Builder
encodeWarcType WarcInfo       = "warcinfo"
encodeWarcType Response       = "response"
encodeWarcType Resource       = "resource"
encodeWarcType Request        = "request"
encodeWarcType Metadata       = "metadata"
encodeWarcType Revisit        = "revisit"
encodeWarcType Conversion     = "conversion"
encodeWarcType Continuation   = "continuation"
encodeWarcType (FutureType t) = encodeText t

newtype Uri = Uri ByteString
            deriving (Show, Read, Eq, Ord)

uri :: Parser Uri
uri = do
    char '<'
    s <- takeTill (== '>')
    char '>'
    return $ Uri s

laxUri :: Parser Uri
laxUri = Uri <$> takeTill (isEndOfLine . ord')

encodeUri :: Uri -> BB.Builder
encodeUri (Uri b) = BB.char7 '<' <> BB.byteString b <> BB.char7 '>'

newtype RecordId = RecordId Uri
                 deriving (Show, Read, Eq, Ord)

recordId :: Parser RecordId
recordId = RecordId <$> uri

encodeRecordId :: RecordId -> BB.Builder
encodeRecordId (RecordId r) = encodeUri r

data TruncationReason = TruncLength
                      | TruncTime
                      | TruncDisconnect
                      | TruncUnspecified
                      | TruncOther !Text
                      deriving (Show, Read, Ord, Eq)

truncationReason :: Parser TruncationReason
truncationReason = choice
    [ "length" *> pure TruncLength
    , "time"   *> pure TruncTime
    , "disconnect" *> pure TruncDisconnect
    , "unspecified" *> pure TruncUnspecified
    , TruncOther <$> utf8Token
    ]

encodeTruncationReason :: TruncationReason -> BB.Builder
encodeTruncationReason TruncLength      = "length"
encodeTruncationReason TruncTime        = "time"
encodeTruncationReason TruncDisconnect  = "disconnect"
encodeTruncationReason TruncUnspecified = "unspecified"
encodeTruncationReason (TruncOther o)   = encodeText o

data Digest = Digest { digestAlgorithm, digestHash :: !ByteString }
            deriving (Show, Read, Eq, Ord)

digest :: Parser Digest
digest = do
    algo <- token <* char ':'
    hash <- token
    return $ Digest algo hash

encodeDigest :: Digest -> BB.Builder
encodeDigest (Digest algo hash) =
    BB.byteString algo <> ":" <> BB.byteString hash

date :: Parser UTCTime
date = do
    s <- takeTill isSpace
    parseTimeM False defaultTimeLocale dateFormat (BS.unpack s)

encodeDate :: UTCTime -> BB.Builder
encodeDate = BB.string7 . formatTime defaultTimeLocale dateFormat

dateFormat = iso8601DateFormat (Just "%H:%M:%SZ")

warcField :: Parser (FieldName, BSL.ByteString)
warcField = do
    fieldName <- FieldName . TE.decodeUtf8 <$> A.takeWhile (/= ':')
    char ':'
    skipSpace
    v0 <- takeLine
    endOfLine
    let continuation :: BB.Builder -> Parser BB.Builder
        continuation v = do
            c <- peekChar
            case c of
              Just c' | isSpace c' -> do
                            v' <- takeLine
                            endOfLine
                            continuation (v <> BB.byteString v')
              _ -> return v
    v1 <- continuation (BB.byteString v0)
    return (fieldName, BB.toLazyByteString v1)

-- | Take the rest of the line (but leaving the newline character unparsed).
takeLine :: Parser BS.ByteString
takeLine = A.takeWhile (isEndOfLine . ord')

data RecordHeader = RecordHeader { _recWarcVersion :: Version
                                 , _recHeaders     :: HM.HashMap FieldName BSL.ByteString
                                 }
                  deriving (Show)

makeLenses ''RecordHeader

addField :: Field a -> a -> RecordHeader -> RecordHeader
addField fld v =
    recHeaders . at (fieldName fld) .~ Just (BB.toLazyByteString $ encode fld v)

-- | A WARC header
header :: Parser RecordHeader
header = withName "header" $ do
    skipSpace
    ver <- version <* endOfLine
    fields <- fmap HM.fromList <$> withName "fields" $ many $ warcField
    endOfLine
    return $ RecordHeader ver fields

encodeHeader :: RecordHeader -> BB.Builder
encodeHeader (RecordHeader (Version maj min) flds) =
       "WARC/"<>BB.intDec maj<>"."<>BB.intDec min <> "\n"
    <> foldMap field (HM.toList flds)
    <> BB.char7 '\n'
  where field :: (FieldName, BSL.ByteString) -> BB.Builder
        field (FieldName fname, value) =
            TE.encodeUtf8Builder fname <> ": " <> BB.lazyByteString value <> BB.char7 '\n'

-- | Lookup the value of a field. Returns @Nothing@ if the field is not
-- present, @Just (Left err)@ in the event of a parse error, and
-- @Just (Right v)@ on success.
lookupField :: RecordHeader -> Field a -> Maybe (Either String a)
lookupField (RecordHeader {_recHeaders=headers}) fld
  | Just v <- HM.lookup (fieldName fld) headers
  = case AL.parse (decode fld) v of
      AL.Fail _ _ err -> Just $ Left err
      AL.Done _ x     -> Just $ Right x
  | otherwise
  = Nothing

data Field a = Field { fieldName :: FieldName
                     , encode    :: a -> BB.Builder
                     , decode    :: Parser a
                     }

mapField :: (a -> b) -> (b -> a) -> Field a -> Field b
mapField f g (Field fieldName encode decode) =
    Field fieldName (encode . g) (f <$> decode)

warcRecordId :: Field RecordId
warcRecordId = Field "WARC-Record-ID" encodeRecordId recordId

contentLength :: Field Integer
contentLength = Field "Content-Length" BB.integerDec decimal

warcDate :: Field UTCTime
warcDate = Field "WARC-Date" encodeDate date

warcType :: Field WarcType
warcType = Field "WARC-Type" encodeWarcType parseWarcType

contentType :: Field BS.ByteString
contentType = Field "Content-Type" BB.byteString (takeTill (isEndOfLine . ord'))

warcConcurrentTo :: Field RecordId
warcConcurrentTo = Field "WARC-Concurrent-To" encodeRecordId recordId

warcBlockDigest :: Field Digest
warcBlockDigest = Field "WARC-Block-Digest" encodeDigest digest

warcPayloadDigest :: Field Digest
warcPayloadDigest = Field "WARC-Payload-Digest" encodeDigest digest

warcIpAddress :: Field BS.ByteString
warcIpAddress = Field "WARC-IP-Address" BB.byteString (takeTill (isEndOfLine . ord'))

warcRefersTo :: Field Uri
warcRefersTo = Field "WARC-Refers-To" encodeUri uri

warcTargetUri :: Field Uri
warcTargetUri = Field "WARC-Target-URI" encodeUri laxUri

warcTruncated :: Field TruncationReason
warcTruncated = Field "WARC-Truncated" encodeTruncationReason truncationReason

warcWarcinfoID :: Field RecordId
warcWarcinfoID = Field "WARC-Warcinfo-ID" encodeRecordId recordId

warcFilename :: Field T.Text
warcFilename = Field "WARC-Filename"(quoted . encodeText) (text <|> quotedString)

warcProfile :: Field Uri
warcProfile = Field "WARC-Profile" encodeUri uri

warcSegmentNumber :: Field Integer
warcSegmentNumber = Field "WARC-Segment-Number" BB.integerDec decimal

warcSegmentTotalLength :: Field Integer
warcSegmentTotalLength = Field "WARC-Segment-Total-Length" BB.integerDec decimal

quoted x = q <> x <> q
  where q = BB.char7 '"'

