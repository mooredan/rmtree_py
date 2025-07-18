-- Media-DupFilesList.sql
/* 2019-02-09 Tom Holden ve3meo

REQUIRES the Duplicate Finder report from CCleaner (or equiv)
processed by spreadsheet "Convert CCleaner Dup Files List to SQLite data" at 
https://docs.google.com/spreadsheets/d/1Ml_CFXBLr9_iJuxBhCrP8wdWk2e4tMkHjkSoqGLcC3M/edit?usp=sharing
into SQL INSERT commands to populate zDupMediaTable for the merging of duplicates.

The zDupMediaTable thus created can then be used by Media_MergeDuplicates.sql
to eliminate the duplicate files and repoint references to them.
*/

-- Combining CCleaner Duplicate Files data with RM database

DROP TABLE IF EXISTS zDupFiles
;
CREATE TEMP TABLE 
zDupFiles (DupID INTEGER PRIMARY KEY, DupGroup INTEGER, MediaID INTEGER, MediaFile TEXT, MediaPath TEXT, SizeKB INTEGER)
;

BEGIN TRANSACTION
;
-- INSERT the SQLite statements from the spreadsheet
-- "Convert CCleaner Dup Files List to SQLite data"
-- in place of those listed below for illustration:

INSERT INTO zDupFiles (DupGroup,MediaFile,MediaPath,SizeKB) VALUES (1,'ons_m19332az-1385 (84E8220AAEC745509E62FF6E3BE28CB3).jpg','C:\Users\ve3me\Documents\RootsMagic\Data\WillcocksTreeShare_media\',REPLACE('372 KB',' KB',''));
INSERT INTO zDupFiles (DupGroup,MediaFile,MediaPath,SizeKB) VALUES (1,'ons_m19332az-1385.jpg','C:\Users\ve3me\Documents\RootsMagic\Data\WillcocksTreeShare_media\',REPLACE('372 KB',' KB',''));

INSERT INTO zDupFiles (DupGroup,MediaFile,MediaPath,SizeKB) VALUES (2,'Willcocks, Isaiah & Hole, Mary - Devon Banns 1821 - FindMyP (56F2DF2C44504F8F8596465E7A0B39A4).jpg','C:\Users\ve3me\Documents\RootsMagic\Data\WillcocksTreeShare_media\',REPLACE('246 KB',' KB',''));
INSERT INTO zDupFiles (DupGroup,MediaFile,MediaPath,SizeKB) VALUES (2,'Willcocks, Isaiah & Hole, Mary - Devon Banns 1821 - FindMyP (F5C74AB315CF464C83C35EC6D13CD447).jpg','C:\Users\ve3me\Documents\RootsMagic\Data\WillcocksTreeShare_media\',REPLACE('246 KB',' KB',''));
INSERT INTO zDupFiles (DupGroup,MediaFile,MediaPath,SizeKB) VALUES (2,'Willcocks, Isaiah & Hole, Mary - Devon Banns 1821 - FindMyP.jpg','C:\Users\ve3me\Documents\RootsMagic\Data\WillcocksTreeShare_media\',REPLACE('246 KB',' KB',''));

INSERT INTO zDupFiles (DupGroup,MediaFile,MediaPath,SizeKB) VALUES (3,'Willcocks, Isaiah & Hole, Mary - Devon Marriages 1821 - Fin (82F4FD282B8345809726A34A37324775).jpg','C:\Users\ve3me\Documents\RootsMagic\Data\WillcocksTreeShare_media\',REPLACE('182 KB',' KB',''));
INSERT INTO zDupFiles (DupGroup,MediaFile,MediaPath,SizeKB) VALUES (3,'Willcocks, Isaiah & Hole, Mary - Devon Marriages 1821 - Fin (832D3502ADC8430D87CF10D63AC58D27).jpg','C:\Users\ve3me\Documents\RootsMagic\Data\WillcocksTreeShare_media\',REPLACE('182 KB',' KB',''));
INSERT INTO zDupFiles (DupGroup,MediaFile,MediaPath,SizeKB) VALUES (3,'Willcocks, Isaiah & Hole, Mary - Devon Marriages 1821 - Fin (B60CA4C66CA6413D919FD869EEEF6E03).jpg','C:\Users\ve3me\Documents\RootsMagic\Data\WillcocksTreeShare_media\',REPLACE('182 KB',' KB',''));
INSERT INTO zDupFiles (DupGroup,MediaFile,MediaPath,SizeKB) VALUES (3,'Willcocks, Isaiah & Hole, Mary - Devon Marriages 1821 - Fin.jpg','C:\Users\ve3me\Documents\RootsMagic\Data\WillcocksTreeShare_media\',REPLACE('182 KB',' KB',''));

COMMIT TRANSACTION
;

-- Get the MediaID from the RM database for each file
UPDATE zDupFiles
SET MediaID =
 (SELECT MediaID FROM MultimediaTable M 
   WHERE zDupFiles.MediaFile = M.MediaFile 
   AND zDupFiles.MediaPath = M.MediaPath
   )
;

-- Duplicate Media List based on matching thumbnails (maybe risky compared to CCleaner and only does image files)
DROP TABLE IF EXISTS zDupMediaTable
;
CREATE TEMP TABLE zDupMediaTable
AS
SELECT M1.MediaID PrimaryID, M1.MediaFile PrimaryFile, M2.MediaID DupID, M2.MediaFile DupFile
FROM zDupFiles M1
LEFT JOIN zDupFiles M2 
USING (DupGroup)   
WHERE M1.MediaID < M2.MediaID
GROUP BY M2.MediaID
;

-- List 
SELECT * FROM zDupMediaTable
;
