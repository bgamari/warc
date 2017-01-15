{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

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
      -- ** Prisms
    , _WarcRecordId
    , _ContentLength
    , _WarcDate
    , _WarcType
    , _ContentType
    , _WarcConcurrentTo
    , _WarcBlockDigest
    , _WarcPayloadDigest
    , _WarcIpAddress
    , _WarcRefersTo
    , _WarcTargetUri
    , _WarcTruncated
    , _WarcWarcinfoId
    , _WarcFilename
    , _WarcProfile
    , _WarcIdentifiedPayloadType
    , _WarcSegmentNumber
    , _WarcSegmentOriginId
    , _WarcSegmentTotalLength
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

import Data.Attoparsec.ByteString.Char8
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Lazy as TL
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as BS
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
                  deriving (Show, Read)

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

fieldName :: Parser FieldName
fieldName = FieldName . TE.decodeUtf8 <$> token

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

warcType :: Parser WarcType
warcType = choice
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

data Field = WarcRecordId !RecordId
           | ContentLength !Integer
           | WarcDate !UTCTime
           | WarcType !WarcType
           | ContentType !ByteString
           | WarcConcurrentTo !RecordId
           | WarcBlockDigest !Digest
           | WarcPayloadDigest !Digest
           | WarcIpAddress !ByteString
           | WarcRefersTo !Uri
           | WarcTargetUri !Uri
           | WarcTruncated !TruncationReason
           | WarcWarcinfoId !RecordId
           | WarcFilename !Text
           | WarcProfile !Uri
           | WarcIdentifiedPayloadType !ByteString
           | WarcSegmentNumber !Integer
           | WarcSegmentOriginId !ByteString
           | WarcSegmentTotalLength !Integer
           deriving (Show, Read)

makePrisms ''Field

date :: Parser UTCTime
date = do
    s <- takeTill isSpace
    parseTimeM False defaultTimeLocale dateFormat (BS.unpack s)

encodeDate :: UTCTime -> BB.Builder
encodeDate = BB.string7 . formatTime defaultTimeLocale dateFormat

dateFormat = iso8601DateFormat (Just "%H:%M:%SZ")

warcField :: Parser Field
warcField = choice
    [ field "WARC-Record-ID" (WarcRecordId <$> recordId)
    , field "Content-Length" (ContentLength <$> decimal)
    , field "WARC-Date" (WarcDate <$> date)
    , field "WARC-Type" (WarcType <$> warcType)
    , field "Content-Type" (ContentType <$> takeTill (isEndOfLine . ord'))
    , field "WARC-Concurrent-To" (WarcConcurrentTo <$> recordId)
    , field "WARC-Block-Digest" (WarcBlockDigest <$> digest)
    , field "WARC-Payload-Digest" (WarcPayloadDigest <$> digest)
    , field "WARC-IP-Address" (WarcIpAddress <$> takeTill (isEndOfLine . ord'))
    , field "WARC-Refers-To" (WarcRefersTo <$> uri)
    , field "WARC-Target-URI" (WarcTargetUri <$> laxUri)
    , field "WARC-Truncated" (WarcTruncated <$> truncationReason)
    , field "WARC-Warcinfo-ID" (WarcWarcinfoId <$> recordId)
    , field "WARC-Filename" (WarcFilename <$> (text <|> quotedString))
    , field "WARC-Profile" (WarcProfile <$> uri)
    -- , field "WARC-Identified-Payload-Type" (WarcIdentifiedPayloadType <$> mediaType)
    , field "WARC-Segment-Number" (WarcSegmentNumber <$> decimal)
    --, field "WARC-Segment-Origin-ID" (WarcSegmentOriginId <$> msgId)
    , field "WARC-Segment-Total-Length" (WarcSegmentTotalLength <$> decimal)
    ]

data RecordHeader = RecordHeader { _recWarcVersion :: Version
                                 , _recHeaders     :: [Field]
                                 }
                  deriving (Show)

makeLenses ''RecordHeader

-- | A WARC header
header :: Parser RecordHeader
header = withName "header" $ do
    skipSpace
    ver <- version <* endOfLine
    let unknownField = field token (takeTill (isEndOfLine . ord') *> return Nothing)
    fields <- withName "fields" $ many $ (Just <$> warcField) <|> unknownField
    endOfLine
    return $ RecordHeader ver (catMaybes fields)

encodeHeader :: RecordHeader -> BB.Builder
encodeHeader (RecordHeader (Version maj min) flds) =
       "WARC/"<>BB.intDec maj<>"."<>BB.intDec min <> "\n"
    <> foldMap encodeField flds
    <> BB.char7 '\n'

encodeField :: Field -> BB.Builder
encodeField fld =
    case fld of
        WarcRecordId r                -> field "WARC-Record-ID" (encodeRecordId r)
        ContentLength len             -> field "Content-Length" (BB.integerDec len)
        WarcDate t                    -> field "WARC-Date" (encodeDate t)
        WarcType t                    -> field "WARC-Type" (encodeWarcType t)
        ContentType t                 -> field "Content-Type" (BB.byteString t)
        WarcConcurrentTo r            -> field "WARC-Concurrent-To" (encodeRecordId r)
        WarcBlockDigest d             -> field "WARC-Block-Digest" (encodeDigest d)
        WarcPayloadDigest d           -> field "WARC-Payload-Digest" (encodeDigest d)
        WarcIpAddress addr            -> field "WARC-IP-Address" (BB.byteString addr)
        WarcRefersTo uri              -> field "WARC-Refers-To" (encodeUri uri)
        WarcTargetUri uri             -> field "WARC-Target-URI" (encodeUri uri)
        WarcTruncated t               -> field "WARC-Truncated" (encodeTruncationReason t)
        WarcWarcinfoId r              -> field "WARC-Warcinfo-ID" (encodeRecordId r)
        WarcFilename n                -> field "WARC-Filename" (quoted $ encodeText n)
        WarcProfile uri               -> field "WARC-Profile" (encodeUri uri)
        WarcSegmentNumber n           -> field "WARC-Segment-Number" (BB.integerDec n)
        WarcSegmentTotalLength len    -> field "WARC-Segment-Total-Length" (BB.integerDec len)
  where
    field :: BB.Builder -> BB.Builder -> BB.Builder
    field name val = name <> ": " <> val <> BB.char7 '\n'

    quoted x = q <> x <> q
      where q = BB.char7 '"'
