import System.IO
import qualified Data.ByteString as BS
import qualified Pipes.ByteString as PBS
import Pipes
import Data.Warc

main :: IO ()
main = do
    let go :: MonadIO m => Record m a -> Producer BS.ByteString m a
        go r = do
            liftIO $ print (recWarcVersion r)
            liftIO $ print (recHeader r)
            encodeRecord r
    rest <- iterRecords (\r -> runEffect $ go r >-> PBS.toHandle stdout) $ parseWarc (PBS.fromHandle stdin)
    --runEffect $ rest >-> PBS.toHandle stdout
    return ()
