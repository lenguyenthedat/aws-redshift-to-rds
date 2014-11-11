{-# LANGUAGE OverloadedStrings, QuasiQuotes, ScopedTypeVariables #-}

module Run where


import           Control.Applicative
import           Control.Exception
import           Control.Monad
import           Data.ByteString                    as BS (ByteString,
                                                           concatMap, pack)
import           Data.Char
import           Data.List
import           Data.String
import           Data.String.Conversions
import           Data.String.Interpolate
import           Database.PostgreSQL.Simple.FromRow
import           GHC.Int
import           System.Exit
import           System.IO

import           DB
import           Options


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
        DROP TABLE IF EXISTS #{to} CASCADE;
        ALTER TABLE #{makeTempTableName to} RENAME TO #{table_name to};
        |]
      return n


data Column = Column {
    column_name :: String,
    data_type :: String,
    character_maximum_length :: Maybe Int,
    numeric_precision :: Maybe Int,
    numeric_precision_radix :: Maybe Int,
    numeric_scale :: Maybe Int
  }
    deriving Show

instance FromRow Column where
    fromRow = Column <$> field <*> field <*> field <*> field <*> field <*> field

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
            numeric_precision_radix,
            numeric_scale
        FROM information_schema.columns
        WHERE
            table_schema ilike ? AND
            table_name ilike ?
        ORDER BY ordinal_position
        |])
        [table_schema table, table_name table]

escapeSingleQuotes :: ByteString -> ByteString
escapeSingleQuotes = BS.concatMap (\ c -> if fromIntegral c == ord '\'' then "''" else pack [c])

makeTempTableName :: Table -> Table
makeTempTableName (Table table_schema table_name) = Table table_schema ("tmp_" ++ table_name)

columnNames :: [Column] -> String
columnNames = intercalate ", " . map columnSelector
  where
    columnSelector :: Column -> String
    columnSelector c | data_type c == "date" = "(" ++ "\"" ++ column_name c ++ "\"" ++ "::varchar)"
    columnSelector c = "\"" ++  column_name c ++ "\""

columnNamesAndTypes :: [Column] -> String
columnNamesAndTypes =
    intercalate ", " . map (\ c -> "\"" ++  column_name c ++ "\"" ++ " " ++ mapType c)

-- | Maps redshift types to rds types.
--   This is by no means a complete mapping. We have to adjust this
--   function in case of new mirrored tables / schema changes.
mapType :: Column -> String
mapType (Column _ "numeric" Nothing (Just precision) _ (Just scale)) =
    [i|numeric(#{precision}, #{scale})|]
mapType (Column _ "character varying" (Just length) Nothing _ Nothing) =
    [i|character varying(#{length})|]
mapType (Column _ "smallint" _ (Just 16) _ (Just 0)) = "smallint"
mapType (Column _ "integer" _ (Just 32) _ (Just 0)) = "integer"
mapType (Column _ "bigint" _ (Just 64) _ (Just 0)) = "bigint"
mapType (Column _ "double precision" Nothing (Just 53) (Just 2) Nothing) =
    "double precision"
mapType (Column _ data_type Nothing Nothing Nothing Nothing)
  | data_type `elem` allowedTypes =
    data_type
  where
    allowedTypes =
        "timestamp without time zone" : "date" : "boolean" :
        []
mapType c = error ("RDS.mapType: invalid type information: " ++ show c)


logMessageLn :: String -> IO ()
logMessageLn = hPutStrLn stderr
