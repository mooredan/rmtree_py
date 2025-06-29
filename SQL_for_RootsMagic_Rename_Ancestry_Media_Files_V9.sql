/* 
2024-03-23 Margaret O'Brien - DataMiningDNA.com 
This script is a modified version of two scripts from Tom Holden 
that generate commands to rename the cryptic media file names. 
This version is intended to be used on a copy of the RootsMagic
media folder, and does not update the database itself.
The original scripts can be found here:
https://sqlitetoolsforrootsmagic.com/TreeShare-Rename-Cryptic-Filenames-for-Citation-Media/
*/

DROP VIEW IF EXISTS NameWay
;
CREATE TEMP VIEW NameWay
AS
SELECT
  CASE WHEN IsPrimary THEN '' ELSE '+' END  
  || CASE WHEN SURNAME NOT LIKE "" THEN SURNAME || ', ' ELSE '_____, ' END 
  || CASE WHEN GIVEN NOT LIKE '' THEN GIVEN || ' ' ELSE '_____ ' END
  -- || BirthYear || '-' DeathYear
  -- DON'T WANT TO DISPLAY ZERO FOR UNKNOWN BIRTH OR DEATH YEAR
  || CASE WHEN BirthYear = 0 THEN '' ELSE BirthYear END 
  || '-'
  || CASE WHEN DeathYear = 0 THEN '' ELSE DeathYear END 
  AS Person  
  , OwnerID AS RIN
  , IsPrimary
  , OwnerID
  , NameID 
FROM NameTable
;

----- Concatenated Names of partners in families in columns Person1 and Person2
DROP VIEW IF EXISTS FamilyWay
;
CREATE TEMP VIEW FamilyWay
AS
SELECT
  IFNULL((SELECT Person FROM NameWay NW WHERE FatherID = NW.OwnerID AND NW.IsPrimary), '_____')
 || ' & ' || 
  IFNULL((SELECT Person FROM NameWay NW WHERE MotherID = NW.OwnerID AND NW.IsPrimary), '_____') AS Waymarks
  ,FamilyID
FROM FamilyTable FT
;
---- Place, PlaceDetail
DROP VIEW IF EXISTS PlaceWay
;
CREATE TEMP VIEW PlaceWay
AS
SELECT
  CASE P1.PlaceType 
   WHEN 0 THEN P1.Name   
   WHEN 2 THEN P2.Name || CHAR(13) || ' ' || P1.Name
   ELSE ''
   END   
   AS Place   
FROM PlaceTable P1
LEFT JOIN PlaceTable P2
ON P1.[MasterID]=P2.[PlaceID]
WHERE P1.PlaceType <> 1
; 

-- Event Pointer
DROP VIEW IF EXISTS EventWay
;
CREATE TEMP View EventWay
AS
SELECT
  CASE E.OwnerType  
    WHEN 0 THEN NW.Person
    WHEN 1 THEN FW.Waymarks
    ELSE 'TBD'    
  END --|| CHAR(13) 
  || ' ' ||  FT.Abbrev || ' ' || IFNULL(SUBSTR(E.Date, 4,4),'') 
  AS Waymarks
  ,EventID
FROM EventTable E
JOIN FactTypeTable FT
ON E.EventType = FT.FactTypeID
LEFT JOIN NameWay NW
ON E.OwnerID = NW.OwnerID
LEFT JOIN FamilyWay FW
ON E.OwnerID = FW.FamilyID
WHERE NW.IsPrimary
;

---- Citation Pointer
DROP VIEW IF EXISTS CitationWay
;
CREATE TEMP View CitationWay
AS
SELECT
  Waymarks
  ,CitationID 
FROM
(
-- Person & Family citations
SELECT
  CASE CL.OwnerType  
    WHEN 0 THEN NW.Person    
    WHEN 1 THEN FW.Waymarks 
    ELSE 'TBD'    
  END || ' INDEX ' || S.Name AS Waymarks
  ,CitationID 
FROM CitationTable C
JOIN SourceTable S USING(SourceID)
JOIN CitationLinkTable CL USING (CitationId)
LEFT JOIN NameWay NW ON CL.OwnerID = NW.OwnerID
LEFT JOIN FamilyWay FW ON CL.OwnerID = FW.FamilyID
WHERE CL.OwnerType IN (0,1)
AND NW.IsPrimary
UNION ALL 
-- Cit pointer for INDI and FAM events
SELECT 
  EW.Waymarks || ' INDEX ' || S.Name AS Waymarks
  ,CitationID 
FROM CitationTable C
JOIN SourceTable S USING(SourceID)
JOIN CitationLinkTable CL USING (CitationId)
JOIN EventWay EW ON CL.OwnerID = EW.EventID
WHERE CL.OwnerType = 2 

UNION ALL 

-- Cit pointer for Alternate Names
SELECT
  NW.Person --|| CHAR(13) 
  -- DON'T WANT THIS IN THE FILE NAME
  /*
  || CASE NW.IsPrimary
     WHEN 1 THEN ' (Name)'
     ELSE ' (Alt Name)'
     END
  */
  || ' INDEX ' || S.Name AS Waymarks
  ,CitationID 
FROM CitationTable C
JOIN SourceTable S USING(SourceID)
JOIN CitationLinkTable CL USING (CitationId)
JOIN NameWay NW ON CL.OwnerID = NW.NameID
WHERE CL.OwnerType = 7 --AND NOT NW.IsPrimary
)
;
--End of CitationWay


-- relate the Ancestry media for citations to the citations
DROP TABLE IF EXISTS zMediaCitationTags
;
CREATE TEMP TABLE zMediaCitationTags
AS
SELECT M.MediaID AS MediaID, ML.LinkID AS LinkID, CW.CitationID AS CitationID, LA.anID AS extID
	, SUBSTR(M.MediaFile, 1, LENGTH(M.MediaFile)) AS MediaFile -- ELIMINATE COLLATION ERROR
	, SUBSTR(M.MediaFile,-3)  AS Ext
	, CW.Waymarks AS WayMarks
FROM AncestryTable LA
JOIN MultiMediaTable M ON LA.rmID = M.MediaID AND LA.LinkType = 11 -- Media
JOIN MediaLinkTable ML USING(MediaID)
JOIN CitationWay CW ON ML.OwnerID = CW.CitationID
WHERE ML.OwnerType = 4 -- Citation
	AND SUBSTR(M.MediaFile, 1, LENGTH(M.MediaFile)) NOT LIKE '%/.jpg' -- ELIMINATE ANCESTRY LINK PROBLEM
ORDER BY M.MediaID, CW.CitationID
;

DROP TABLE IF EXISTS zMediaNewNames
;

CREATE TEMP TABLE zMediaNewNames
AS
SELECT 
MediaID, MediaFile AS OldName, WayMarks || '@' || MediaFile AS NewName
FROM zMediaCitationTags
WHERE LOWER(MediaFile) REGEXP LOWER(extID) || '\..+'  --test that extID and filename match, not previously renamed
GROUP BY MediaID;


SELECT 'RENAME ' || ' "' || OldName || '" "' || NewName ||'"' AS CMD
FROM zMediaNewNames
ORDER BY NewName
;
