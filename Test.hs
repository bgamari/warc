import Pipes
import qualified Pipes.Prelude as PP
import Data.Warc
import Data.Warc.Header
import System.IO
import qualified Pipes.ByteString as PBS
import qualified Data.ByteString as BS
import Control.Lens

main :: IO ()
main = testWhole

-- | Example of streaming record bodies.
testStreaming :: IO ()
testStreaming = do
    let go :: MonadIO m => Record m a -> m a
        go r = do
            liftIO $ putStrLn $ replicate 80 '='
            liftIO $ print (recHeader r ^. recWarcVersion)
            liftIO $ print (recHeader r ^. recHeaders)
            liftIO $ putStrLn $ replicate 80 '-'
            runEffect $ recContent r >-> PBS.toHandle stdout

    let warc :: Warc IO ()
        warc = parseWarc $ PBS.fromHandle stdin
    rest <- iterRecords go warc
    -- print the leftovers
    runEffect $ rest >-> PBS.toHandle stdout

-- | Example of reading whole records in non-streaming fashion
testWhole :: IO ()
testWhole = do
    let -- | Maps each document to its length
        docLength :: MonadIO m
                  => RecordHeader
                  -> Producer BS.ByteString m b
                  -> Producer Int m b
        docLength hdr body = do
            (len, rest) <- lift $ PP.fold' (+) 0 id $ body >-> PP.map BS.length
            yield len
            return rest

    let warc :: Warc IO ()
        warc = parseWarc (PBS.fromHandle stdin)

    runEffect $ do
        rest <- produceRecords docLength warc >-> PP.mapM_ (liftIO . print)
        -- print the leftovers
        rest >-> PBS.toHandle stdout
        return ()
