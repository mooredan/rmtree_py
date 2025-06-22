--TreeShare-MediaRenameStep2.sql
/* 
2017-07-05 Tom Holden ve3meo
Used after TreeShare-MediaRenameStep1.sql 
Requires the temp table zMediaCitationTags created by that script so the
database must remain open in the SQLite manager.

Generates the temp table zMediaNewNames which pairs the cryptic media file names
downloaded from Ancestry with new, informative filenames. Changes the 
media links to these new names and generates commands in temp table zMediRenameCMD
for a batch file to rename the media files.

Temp tables, temp views are all lost when the SQLite manager closes the database. It
returns to looking like a normal database.
*/



-- pair up the Ancestry media filenames with new filenames 
DROP TABLE IF EXISTS zMediaNewNames
;

CREATE TEMP TABLE zMediaNewNames
AS
SELECT 
MediaID, MediaFile AS OldName, WayMarks || '@' || MediaFile AS NewName
FROM zMediaCitationTags
WHERE LOWER(MediaFile) REGEXP LOWER(extID) || '\..+'  --test that extID and filename match, not previously renamed
GROUP BY MediaID;


-- generate Windows RENAME commands
DROP TABLE IF EXISTS zMediaRenameCMD
;

CREATE TEMP TABLE zMediaRenameCMD
AS
SELECT 'RENAME ' || ' "' || OldName || '" "' || NewName ||'"' AS CMD
FROM zMediaNewNames
;

-- change the media links to the new names
UPDATE MultiMediaTable 
SET MediaFile = (SELECT NewName FROM zMediaNewNames z WHERE MultimediaTable.MediaID = z.MediaID)
WHERE MediaID IN (SELECT DISTINCT MediaID FROM zMediaNewNames)
;

-- instructions
SELECT
'DO NOT CLOSE the SQLite manager until you are finished renaming or undoing.

Inspect temp tables zMedia... for unacceptable anomalies which you may be
able to fix... Your Media Gallery will now show broken links for the downloaded
files from Ancestry.com.

To fix these broken links, copy the batch commands from zMediaRenameCMD to a 
text editor. Delete the 1st line "CMD".
Save the text file as a .BAT or .CMD file to the media folder having the
same name as the database file and is a subfolder of its folder.
Open the Command Line Interpreter on this media folder and execute the file.

            ---------UNDO---------------
DO NOT CLOSE the database in the SQLite manager if you wish to undo the 
renamimg and relinking.

You can undo by executing script TreeShare-MediaRenameUNDO.sql and the batch 
commands it generates.' 
AS Instructions

 