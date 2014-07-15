{-# LANGUAGE QuasiQuotes, ScopedTypeVariables, OverloadedStrings #-}

module Run where


import Data.String.Conversions
import Data.ByteString as BS (ByteString, concatMap, pack)
import Data.List
import System.IO
import Data.Char
import Control.Applicative
import System.Exit
import Control.Exception
import Data.String.Interpolate
import Database.PostgreSQL.Simple.FromRow
import Data.String
import GHC.Int
import Control.Monad

import Options
import DB


run :: [String] -> IO ExitCode
run args = withOptions args $ \ options -> do
  case (fromType options, toType options) of
    (Redshift, Rds) -> do
      logMessageLn [i|mirroring from redshift to RDS: #{fromTable options} -> #{toTable options}...|]
      n <- mirror
        (fromConnectionString options)
        (toConnectionString options)
        (fromTable options)
        (toTable options)
      logMessageLn [i|copied #{n} rows.|]
      return ExitSuccess
    _ -> throwIO $ ErrorCall
      [i|mirroring from #{fromType options} to #{toType options} is not yet supported.|]

mirror :: String -> String -> Table -> Table -> IO Int64
mirror redshiftConnectionString rdsConnectionString from to = do
  sourceColumns :: [Column] <- withRedshift redshiftConnectionString $
    readColumns from
  when (null sourceColumns) $
    throwIO $ ErrorCall [i|table #{from} does not exist|]
  withRds rdsConnectionString $ \ rds -> do
    withTransaction rds $ do
      _ <- execute_ rds "CREATE EXTENSION IF NOT EXISTS dblink;"
      n <- execute_ rds $ fromString $ [i|
        SELECT dblink_connect('#{escapeSingleQuotes $ cs redshiftConnectionString}');
        SELECT tmp_#{table_name to}.* INTO #{makeTempTableName to}
        FROM dblink('SELECT #{columnNames sourceColumns} FROM #{from}')
          AS tmp_#{table_name to}(#{columnNamesAndTypes sourceColumns});
        |]
      _ <- execute_ rds $ fromString $ [i|
        DROP TABLE IF EXISTS #{to};
        ALTER TABLE #{makeTempTableName to} RENAME TO #{table_name to};
        |]
      return n


data Column = Column {
    column_name :: String,
    data_type :: String,
    character_maximum_length :: Maybe Int,
    numeric_precision :: Maybe Int,
    numeric_scale :: Maybe Int
  }
    deriving Show

instance FromRow Column where
    fromRow = Column <$> field <*> field <*> field <*> field <*> field

withRedshift :: String -> (Connection -> IO a) -> IO a
withRedshift connectionString = bracket
  (connectPostgreSQL $ cs connectionString)
  close

withRds :: String -> (Connection -> IO a) -> IO a
withRds connectionString = bracket
  (connectPostgreSQL $ cs connectionString)
  close

readColumns :: Table -> Connection -> IO [Column]
readColumns table db = do
    query db (fromString [i|
        SELECT
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns
        WHERE
            table_schema = ? AND
            table_name = ?
        ORDER BY ordinal_position
        |])
        [table_schema table, table_name table]

escapeSingleQuotes :: ByteString -> ByteString
escapeSingleQuotes = BS.concatMap (\ c -> if fromIntegral c == ord '\'' then "''" else pack [c])

makeTempTableName :: Table -> Table
makeTempTableName (Table table_schema table_name) = Table table_schema ("tmp_" ++ table_name)

columnNames :: [Column] -> String
columnNames = intercalate ", " . map column_name

columnNamesAndTypes :: [Column] -> String
columnNamesAndTypes =
    intercalate ", " . map (\ c -> column_name c ++ " " ++ mapType c)

-- | Maps redshift types to rds types.
--   This is by no means a complete mapping. We have to adjust this
--   function in case of new mirrored tables / schema changes.
mapType :: Column -> String
mapType (Column _ "numeric" Nothing (Just precision) (Just scale)) =
    [i|numeric(#{precision}, #{scale})|]
mapType (Column _ "character varying" (Just length) Nothing Nothing) =
    [i|character varying(#{max 1 length})|]
mapType (Column _ "smallint" _ (Just 16) (Just 0)) = "smallint"
mapType (Column _ "integer" _ (Just 32) (Just 0)) = "integer"
mapType (Column _ "bigint" _ (Just 64) (Just 0)) = "bigint"
mapType (Column _ data_type Nothing Nothing Nothing) | data_type `elem` allowedTypes =
    data_type
  where
    allowedTypes =
        "timestamp without time zone" : "date" : "boolean" :
        []
mapType c = error ("RDS.mapType: invalid type information: " ++ show c)


logMessageLn :: String -> IO ()
logMessageLn = hPutStrLn stderr
