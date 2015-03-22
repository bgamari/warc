{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Data.Warc.Header
    ( Version(..)
    , WarcType (..)
    , RecordId (..)
    , TruncationReason (..)
    , Digest (..)
    , header
      -- * Header field types
    , Field (..)
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
    ) where

import Control.Applicative
import Control.Monad (void)
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

import Control.Lens

data Version = Version {versionMajor, versionMinor :: !Int}
             deriving (Show, Read, Eq, Ord)

version :: Parser Version
version = do
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

field :: ByteString -> Parser a -> Parser a
field name content = do
    try $ string name
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
     
newtype RecordId = RecordId ByteString
                 deriving (Show, Read, Eq, Ord)

uri :: Parser ByteString
uri = do
    char '<'
    s <- takeTill (== '>')
    char '>'
    return s

recordId :: Parser RecordId
recordId = RecordId <$> uri

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

data Digest = Digest { digestAlgorithm, digestHash :: !ByteString }
            deriving (Show, Read, Eq, Ord)

digest :: Parser Digest
digest = do
    algo <- token <* char ':'
    hash <- token
    return $ Digest algo hash

data Field = WarcRecordId !RecordId
           | ContentLength !Integer
           | WarcDate !UTCTime
           | WarcType !WarcType
           | ContentType !ByteString
           | WarcConcurrentTo [RecordId]
           | WarcBlockDigest !Digest
           | WarcPayloadDigest !Digest
           | WarcIpAddress !ByteString
           | WarcRefersTo !ByteString
           | WarcTargetUri !ByteString
           | WarcTruncated !TruncationReason
           | WarcWarcinfoId !RecordId
           | WarcFilename !Text
           | WarcProfile !ByteString
           | WarcIdentifiedPayloadType !ByteString
           | WarcSegmentNumber !Integer
           | WarcSegmentOriginId !ByteString
           | WarcSegmentTotalLength !Integer
           deriving (Show, Read)

makePrisms ''Field

date :: Parser UTCTime
date = do
    s <- takeTill isSpace
    parseTimeM False defaultTimeLocale fmt (BS.unpack s)
  where fmt = iso8601DateFormat (Just "%H:%M:%SZ")
           
warcField :: Parser Field
warcField = choice
    [ field "WARC-Record-ID" (WarcRecordId <$> recordId)
    , field "Content-Length" (ContentLength <$> decimal)
    , field "WARC-Date" (WarcDate <$> date)
    , field "WARC-Type" (WarcType <$> warcType)
    , field "Content-Type" (ContentType <$> takeTill (isEndOfLine . ord'))
    , field "WARC-Concurrent-To" (WarcConcurrentTo <$> many1 recordId)
    , field "WARC-Block-Digest" (WarcBlockDigest <$> digest)
    , field "WARC-Payload-Digest" (WarcPayloadDigest <$> digest)
    , field "WARC-IP-Address" (WarcIpAddress <$> takeTill (isEndOfLine . ord'))
    , field "WARC-Refers-To" (WarcRefersTo <$> uri)
    , field "WARC-Target-URI" (WarcTargetUri <$> takeTill (isEndOfLine . ord'))
    , field "WARC-Truncated" (WarcTruncated <$> truncationReason)
    , field "WARC-Warcinfo-ID" (WarcWarcinfoId <$> recordId)
    , field "WARC-Filename" (WarcFilename <$> (text <|> quotedString))
    , field "WARC-Profile" (WarcProfile <$> uri)
    -- , field "WARC-Identified-Payload-Type" (WarcIdentifiedPayloadType <$> mediaType)
    , field "WARC-Segment-Number" (WarcSegmentNumber <$> decimal)
    --, field "WARC-Segment-Origin-ID" (WarcSegmentOriginId <$> msgId)
    , field "WARC-Segment-Total-Length" (WarcSegmentTotalLength <$> decimal)
    ]

-- | A WARC header
header :: Parser (Version, [Field])
header = do
    ver <- version <* endOfLine
    fields <- many warcField
    endOfLine
    return (ver, fields)
