{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GADTs #-}

-- | WARC (or Web ARCive) is a archival file format widely used to distribute
-- corpora of crawled web content (see, for instance the Common Crawl corpus). A
-- WARC file consists of a set of records, each of which describes a web request
-- or response.
--
-- This module provides a streaming parser and encoder for WARC archives for use
-- with the @pipes@ package.
--
module Data.Warc
    ( Warc(..)
    , Record(..)
      -- * Parsing
    , parseWarc
    , iterRecords
    , produceRecords
      -- * Encoding
    , encodeRecord
      -- * Headers
    , module Data.Warc.Header
    ) where

import Data.Char (ord)
import Pipes hiding (each)
import qualified Pipes.ByteString as PBS
import Control.Lens
import qualified Pipes.Attoparsec as PA
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy.Builder as BB
import Data.ByteString (ByteString)
import Control.Monad (join)
import Control.Monad.Trans.Free
import Control.Monad.Trans.State.Strict

import Data.Warc.Header


-- | A WARC record
--
-- This represents a single record of a WARC file, consisting of a set of
-- headers and a means of producing the record's body.
data Record m r = Record { recHeader    :: RecordHeader
                           -- ^ the WARC headers
                         , recContent   :: Producer BS.ByteString m r
                           -- ^ the body of the record
                         }

instance Monad m => Functor (Record m) where
    fmap f (Record hdr r) = Record hdr (fmap f r)

-- | A WARC archive.
--
-- This represents a sequence of records followed by whatever data
-- was leftover from the parse.
type Warc m a = FreeT (Record m) m (Producer BS.ByteString m a)

-- | Parse a WARC archive.
--
-- Note that this function does not actually do any parsing itself;
-- it merely returns a 'Warc' value which can then be run to parse
-- individual records.
parseWarc :: (Functor m, Monad m)
          => Producer ByteString m a   -- ^ a producer of a stream of WARC content
          -> Warc m a                  -- ^ the parsed WARC archive
parseWarc = loop
  where
    loop upstream = FreeT $ do
        (hdr, rest) <- runStateT (PA.parse header) upstream
        go hdr rest

    go mhdr rest
      | Nothing <- mhdr             = return $ Pure rest
      | Just (Left err) <- mhdr     = error $ show err
      | Just (Right hdr) <- mhdr
      , Just len <- hdr ^? recHeaders . each . _ContentLength = do
            let produceBody = fmap consumeWhitespace . view (PBS.splitAt len)
                consumeWhitespace = PBS.dropWhile isEOL
                isEOL c = c == ord8 '\r' || c == ord8 '\n'
                ord8 = fromIntegral . ord
            return $ Free $ Record hdr $ fmap loop $ produceBody rest

-- | Iterate over the 'Record's in a WARC archive
iterRecords :: forall m a. Monad m
            => (forall b. Record m b -> m b)  -- ^ the action to run on each 'Record'
            -> Warc m a                       -- ^ the 'Warc' file
            -> m (Producer BS.ByteString m a) -- ^ returns any leftover data
iterRecords f warc = iterT iter warc
  where
    iter :: Record m (m (Producer BS.ByteString m a))
         -> m (Producer BS.ByteString m a)
    iter r = join $ f r

produceRecords :: forall m o a. Monad m
               => (forall b. RecordHeader -> Producer BS.ByteString m b
                                          -> Producer o m b)
                  -- ^ consume the record producing some output
               -> Warc m a
                  -- ^ a WARC archive (see 'parseWarc')
               -> Producer o m (Producer BS.ByteString m a)
                  -- ^ returns any leftover data
produceRecords f warc = iterTM iter warc
  where
    iter :: Record m (Producer o m (Producer BS.ByteString m a))
         -> Producer o m (Producer BS.ByteString m a)
    iter (Record hdr body) = join $ f hdr body

-- | Encode a 'Record' in WARC format.
encodeRecord :: Monad m => Record m a -> Producer BS.ByteString m a
encodeRecord (Record hdr content) = do
    PBS.fromLazy $ BB.toLazyByteString $ encodeHeader hdr
    content
