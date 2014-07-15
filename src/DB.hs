
module DB (
    module Reexported,
    execute_,
    query,
  ) where


import Prelude hiding (log)
import System.IO
import qualified Database.PostgreSQL.Simple as P
import Database.PostgreSQL.Simple as Reexported hiding (execute_, query)
import GHC.Int
import Control.Monad
import Data.String.Conversions
import Database.PostgreSQL.Simple.Types (fromQuery)


debug :: Bool
debug = False

execute_ :: P.Connection -> P.Query -> IO Int64
execute_ db q = do
  log q
  P.execute_ db q

query :: (ToRow q, FromRow r) => Connection -> Query -> q -> IO [r]
query db q vs = do
  log q
  P.query db q vs

log :: Query -> IO ()
log q =
  when debug $
    hPutStrLn stderr $ cs $ fromQuery q
