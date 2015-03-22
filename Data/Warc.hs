{-# LANGUAGE RankNTypes #-}

module Data.Warc
    ( Record(..)
    , Warc(..)
    , parseWarc
    , iterRecords
    ) where

import Pipes (Producer, yield)
import qualified Pipes.ByteString as PBS
import Control.Lens
import qualified Pipes.Attoparsec as PA
import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import Control.Monad (void)
import Control.Monad.Trans.Class
import Control.Monad.Trans.Either
import Control.Monad.Trans.Free
import Control.Monad.Trans.State.Strict
import Control.Error

import Data.Warc.Header

-- | A WARC record
data Record m r = Record { recWarcVersion :: Version
                         , recHeader      :: [Field]
                         , recContent     :: Producer BS.ByteString m r
                         }

instance Monad m => Functor (Record m) where
    fmap f (Record ver hdr r) = Record ver hdr (fmap f r)

-- | A WARC archive
data Warc m = Warc (FreeT (Record m) m (Producer BS.ByteString m ()))

instance Monad m => Monoid (Warc m) where
    Warc a `mappend` Warc b = Warc (a >> b)
    mempty = Warc (return (return ()))

parseWarc :: (Functor m, Monad m) => Producer ByteString m () -> Warc m
parseWarc = Warc . loop
  where
    loop upstream = FreeT $ do
        (hdr, rest) <- runStateT (PA.parse header) upstream
        go hdr rest

    go hdr rest
      | Nothing <- hdr                    = return $ Pure rest
      | Just (Left err) <- hdr            = error $ show err
      | Just (Right (ver, fields)) <- hdr = do
            let [len] = toListOf (each . _ContentLength) fields
            let produceBody = rest ^. PBS.splitAt len
            return $ Free $ Record ver fields $ fmap loop produceBody

iterRecords :: Monad m => (forall a. Record m a -> m a) -> Warc m -> m (Producer BS.ByteString m ())
iterRecords f (Warc free) = go free
  where
    go (FreeT action) = action >>= \next -> do
        case next of
          Pure a -> return a
          Free record -> do
              rest <- f record
              go rest
