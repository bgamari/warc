import Pipes
import Data.Warc
import Data.Warc.Header
import Data.Attoparsec.ByteString.Char8
import qualified Data.ByteString as BS
import System.IO
import qualified Pipes.ByteString as PBS
import Control.Monad.IO.Class

testHeader = do
    f <- BS.readFile "bbc.warc"
    print $ parse header f

testIter = do
    let go :: MonadIO m => Record m a -> m a
        go r = do
            liftIO $ print (recWarcVersion r)
            liftIO $ print (recHeader r)
            runEffect $ recContent r >-> PBS.toHandle stdout
    rest <- iterRecords go $ parseWarc (PBS.fromHandle stdin)
    runEffect $ rest >-> PBS.toHandle stdout

main = testIter
