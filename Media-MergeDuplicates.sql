-- Media-MergeDuplicates.sql

/* 2019-02-08 Tom Holden ve3meo
Requires zDupMediaTable to be generated first from either:
Media-DupThumbnailsList.sql
OR
Media-DupFilesList.sql
*/

-- Set Modified to 1 for LinkAncestryTable Citation records affected
-- before MediaLinkTable gets changed
-- Set All Persons in TreeShare to Unchanged
UPDATE LinkAncestryTable SET Modified = 0
;

DROP VIEW IF EXISTS CitMods
;
CREATE TEMP VIEW CitMods
AS
SELECT DISTINCT ML.OwnerID AS CitationID 
FROM zDupMediaTable z
JOIN MediaLinkTable ML
ON z.DupID = ML.MediaID
AND ML.OwnerType = 4
;
UPDATE LinkAncestryTable SET Modified = 1
WHERE rmID IN (SELECT CitationID FROM CitMods)
AND LinkType = 4
;

-- Persons with Personal dup media
DROP VIEW IF EXISTS PersMods
;
CREATE TEMP VIEW PersMods
AS
SELECT DISTINCT ML.OwnerID AS PersonID 
FROM zDupMediaTable z
JOIN MediaLinkTable ML
ON z.DupID = ML.MediaID
AND ML.OwnerType = 0
;

-- Persons whose Citation Media is Dup Media

---- Get CitationTable.OwnerType and OwnerID
DROP VIEW IF EXISTS CitModsOwners
;
CREATE TEMP VIEW CitModsOwners
AS
SELECT C.CitationID, C.OwnerID, C.OwnerType
FROM LinkAncestryTable LAT
JOIN CitationTable C
ON LAT.rmID = C.CitationID
AND LAT.LinkType = 4
AND LAT.Modified
;

---- Get PersonIDs of ultimate persons owning citations
DROP VIEW IF EXISTS CitModsPersons
;
CREATE TEMP VIEW CitModsPersons
AS
SELECT OwnerID AS PersonID
FROM CitModsOwners
WHERE OwnerType = 0 -- personal citations
UNION
SELECT F.FatherID AS PersonID -- husband
FROM FamilyTable F
JOIN CitModsOwners CMO
ON CMO.OwnerID = F.FamilyID
AND CMO.OwnerType = 1 -- family citations
AND F.FatherID -- not 0
UNION
SELECT F.MotherID AS PersonID -- wife
FROM FamilyTable F
JOIN CitModsOwners CMO
ON CMO.OwnerID = F.FamilyID
AND CMO.OwnerType = 1  -- family citations
AND F.MotherID -- not 0
UNION
SELECT E.OwnerID AS PersonID -- person having event with citation
FROM EventTable E
JOIN CitModsOwners CMO
ON CMO.OwnerID = E.EventID
AND CMO.OwnerType = 2 -- event
AND E.OwnerType = 0 -- individual event 
UNION
SELECT F.FatherID AS PersonID -- husband having family event with citation
FROM FamilyTable F
JOIN EventTable E
ON E.OwnerID = F.FamilyID
AND E.OwnerType = 1
AND F.FatherID >0 
JOIN CitModsOwners CMO
ON CMO.OwnerID = E.EventID
AND CMO.OwnerType = 2 -- event
AND E.OwnerType = 1 -- family event 
UNION
SELECT F.MotherID AS PersonID -- wife having family event with citation
FROM FamilyTable F
JOIN EventTable E
ON E.OwnerID = F.FamilyID
AND E.OwnerType = 1
AND F.MotherID >0
JOIN CitModsOwners CMO
ON CMO.OwnerID = E.EventID
AND CMO.OwnerType = 2 -- event
AND E.OwnerType = 1 -- family event 
;

UPDATE LinkAncestryTable SET Modified = 1
WHERE rmID IN 
 (SELECT * FROM PersMods
  UNION
  SELECT * FROM CitModsPersons
  )
AND LinkType = 0
;

-- Media records
UPDATE LinkAncestryTable SET Modified=1
WHERE LinkType=11  -- Media
AND rmID IN (SELECT DISTINCT DupID FROM zDupMediaTable)
; 
---- Finished setting LinkAncestryTable records to Modified = 1


-- repoint MediaLinks from DupID to PrimaryID
UPDATE MediaLinkTable
SET MediaID = (SELECT PrimaryID FROM zDupMediaTable Z WHERE MediaID = Z.DupID)
WHERE MediaID IN (SELECT DISTINCT DupID FROM zDupMediaTable)
;


-- Generate CLI commands to delete Dup Media files
DROP TABLE IF EXISTS zDupMediaDelete
;
CREATE TEMP TABLE zDupMediaDelete
AS
SELECT
 'ERASE "'
  || CASE substr(MediaPath,-1)
      WHEN '\'
       THEN MediaPath 
      ELSE MediaPath || '\' 
     END 
  || MediaFile 
  || '"' 
 AS CMD  -- MEDIAPATH INCONSISTENTLY ENDS WITH \ ####
FROM MultimediaTable
WHERE MediaID IN (SELECT DupID FROM zDupMediaTable)
ORDER BY CMD
;

-- Remove Dup Media from Gallery
DELETE FROM MultimediaTable
WHERE MediaID IN (SELECT DupID FROM zDupMediaTable)
;

-- List DELETE commands
SELECT 
'REM Links to Duplicate media have been merged in this database.
REM If confident that the files themselves are unused elsewhere,
REM copy the commands below into the Windows or OS Command shell
REM and execute to delete these duplicate files from the computer
REM storage system.' AS CMD
UNION ALL
SELECT * FROM zDupMediaDelete
;
-- END of SCRIPT
