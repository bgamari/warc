import Warc
import Data.Attoparsec.ByteString.Char8
import qualified Data.ByteString as BS

main = do
    f <- BS.readFile "bbc.warc"
    print $ parse header f
