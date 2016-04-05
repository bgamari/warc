{-# LANGUAGE OverloadedStrings #-}

import Data.List (isSuffixOf)
import Control.Applicative
import System.IO
import System.FilePath

import Data.Attoparsec.ByteString.Char8
import qualified Data.Text as T
import qualified Data.ByteString as BS
import Control.Monad.IO.Class
import Control.Monad.Catch

import Control.Lens
import Data.Text.Lens
import Pipes hiding (each)
import qualified Pipes.ByteString as PBS
import qualified Pipes.GZip as GZip
import qualified Pipes.Prelude as PP
import Data.Warc
import Data.Warc.Header
import Options.Applicative as O hiding (header)

withFileProducer :: (MonadIO m, MonadMask m)
                 => FilePath
                 -> (Producer BS.ByteString m () -> m a)
                 -> m a
withFileProducer path action = bracket (liftIO $ openFile path ReadMode) (liftIO . hClose) $ \h ->
    let prod
          | ".gz" `isSuffixOf` path = hoist liftIO $ GZip.decompress $ PBS.fromHandle h
          | otherwise               = PBS.fromHandle h
    in action prod

testHeader = do
    f <- BS.readFile "bbc.warc"
    print $ parse header f

outFile :: Record m a -> Maybe FilePath
outFile r = fileName <|> recId
  where
    fileName = recHeader r ^? recHeaders . each . _WarcFilename . _Text
    recId = recHeader r ^? recHeaders . each . _WarcRecordId . to recIdToFileName
    recIdToFileName (RecordId (Uri uri)) = "hello"

doExport :: FilePath -> FilePath -> IO ()
doExport outDir warcPath = do
    let go :: Record IO a -> IO a
        go r =
            case outFile r of
                Nothing -> runEffect $ recContent r >-> PP.drain
                Just outName -> withFile (outDir </> outName) WriteMode $ \outH -> do
                    putStrLn outName
                    runEffect $ recContent r >-> PBS.toHandle outH

    withFileProducer warcPath $ \prod -> do
        rest <- iterRecords go $ parseWarc prod
        putStrLn "That was all. This is left over..."
        runEffect $ rest >-> PBS.toHandle stdout
    return ()


args :: O.Parser (IO ())
args =
    doExport <$> O.option str (short 'C' <> long "directory" <> metavar "DIR"
                               <> help "Directory to place output in")
             <*> O.argument str (metavar "FILE" <> help "The WARC file to export")

main :: IO ()
main = do
    action <- execParser (info (helper <*> args) mempty)
    action
