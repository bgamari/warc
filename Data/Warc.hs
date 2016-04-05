{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GADTs #-}

module Data.Warc
    ( Record(..)
    , Warc(..)
      -- * Parsing
    , parseWarc
    , iterRecords
      -- * Encoding
    , encodeRecord
    ) where

import Data.Char (ord)
import Pipes hiding (each)
import qualified Pipes.ByteString as PBS
import qualified Pipes.Prelude as PP
import Control.Lens
import qualified Pipes.Attoparsec as PA
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Builder as BB
import Data.ByteString (ByteString)
import Control.Monad (join)
import Control.Monad.Trans.Free
import Control.Monad.Trans.State.Strict
import Control.Monad.Trans.Class

import Data.Warc.Header


-- | A WARC record
data Record m r = Record { recHeader    :: RecordHeader
                         , recContent   :: Producer BS.ByteString m r
                         }

instance Monad m => Functor (Record m) where
    fmap f (Record hdr r) = Record hdr (fmap f r)

-- | A WARC archive
type Warc m a = FreeT (Record m) m (Producer BS.ByteString m a)

-- | Parse a WARC archive.
parseWarc :: (Functor m, Monad m)
          => Producer ByteString m a
          -> Warc m a
parseWarc = loop
  where
    loop upstream = FreeT $ do
        (hdr, rest) <- runStateT (PA.parse header) upstream
        go hdr rest

    go mhdr rest
      | Nothing <- mhdr             = return $ Pure rest
      | Just (Left err) <- mhdr     = error $ show err
      | Just (Right hdr) <- mhdr
      , Just len <- recHeaders hdr ^? each . _ContentLength = do
            let produceBody = fmap consumeWhitespace . view (PBS.splitAt len)
                consumeWhitespace = PBS.dropWhile isEOL
                isEOL c = c == ord8 '\r' || c == ord8 '\n'
                ord8 = fromIntegral . ord
            return $ Free $ Record hdr $ fmap loop $ produceBody rest

-- | Iterate over the 'Record's in a WARC archive
iterRecords :: forall m a. Monad m
            => (forall b. Record m b -> m b)
            -> Warc m a
            -> m (Producer BS.ByteString m a)
iterRecords f warc = iterT iter warc
  where
    iter :: Record m (m (Producer BS.ByteString m a))
         -> m (Producer BS.ByteString m a)
    iter r = join $ f r

-- | A produce
produceRecords :: forall m o a. Monad m
               => (RecordHeader -> Consumer' BS.ByteString m o)  -- ^ consume a record
               -> Warc m a -- ^ a WARC archive (see 'parseWarc')
               -> Producer o m (Producer BS.ByteString m a) -- ^ returns any leftovers
produceRecords f warc = iterTM iter warc
  where
    iter :: Record m (Producer o m (Producer BS.ByteString m a))
         -> Producer o m (Producer BS.ByteString m a)
    iter (Record hdr body) = do
        let f' :: Pipe BS.ByteString o m r
            f' = (f hdr >>= yield) >> PP.drain
        join $ body >-> f'

encodeRecord :: Monad m => Record m a -> Producer BS.ByteString m a
encodeRecord (Record hdr content) = do
    PBS.fromLazy $ BB.toLazyByteString $ encodeHeader hdr
    content
