-- -- TreeShare-MediaRenameStep1.sql
/*
2017-07-05 Tom Holden ve3meo
Adapted from RM7_5_WaymarksViews.sql to rename TreeShare downloaded
media files from coded names to meaningful ones. 

Creates temporary views of all or most tables in the RootsMagic 6
database with the first column filled with Waymarks to aid in 
navigating through RootsMagic to the screen that controls the 
data contained in that table.

Requires a SQLite manager with a RMNOCASE collation sequence.

*/

-----Names of Persons concatenated into one column, "Person"
DROP VIEW IF EXISTS NameWay
;
CREATE TEMP VIEW NameWay
AS
SELECT
  CASE WHEN IsPrimary THEN '' ELSE '+' END  
--  || CASE WHEN PREFIX NOT LIKE '' THEN PREFIX || ' ' ELSE '' END  
  || CASE WHEN SURNAME NOT LIKE "" THEN SURNAME || ', ' ELSE '_____, ' END 
  || CASE WHEN GIVEN NOT LIKE '' THEN GIVEN || ' ' ELSE '_____ ' END
  || BirthYear || '-' || DeathYear 
--  || CASE WHEN NICKNAME NOT LIKE '' THEN '"'|| NICKNAME || '" ' ELSE '' END 
--  || CASE WHEN SUFFIX NOT LIKE '' THEN ', '|| SUFFIX ELSE '' END 
--  || '-' || OwnerID  
  AS Person  
  , OwnerID AS RIN
  , *
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
 , FT.* 
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
  , P1.*
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
  , E.*    
FROM EventTable E
JOIN FactTypeTable FT
ON E.EventType = FT.FactTypeID
LEFT JOIN NameWay NW
ON E.OwnerID = NW.OwnerID
LEFT JOIN FamilyWay FW
ON E.OwnerID = FW.FamilyID
WHERE NW.IsPrimary
;

-- FactType Pointer
DROP VIEW IF EXISTS FactTypeWay
;
CREATE TEMP VIEW FactTypeWay
AS
SELECT
  'Fact Type' || CHAR(13) || ' ' || Name AS Waymarks
  , * 
FROM FactTypeTable
;

---- Citation Pointer
DROP VIEW IF EXISTS CitationWay
;
CREATE TEMP View CitationWay
AS
SELECT
  *  
FROM
(
-- Person & Family citations
SELECT
  CASE C.OwnerType  
    WHEN 0 THEN NW.Person    
    WHEN 1 THEN FW.Waymarks 
    ELSE 'TBD'    
  END || ' ' || S.Name AS Waymarks
  , C.*  
FROM CitationTable C
JOIN SourceTable S
USING(SourceID)
LEFT JOIN NameWay NW
ON C.OwnerID = NW.OwnerID
LEFT JOIN FamilyWay FW
ON C.OwnerID = FW.FamilyID
WHERE C.OwnerType IN (0,1)
AND NW.IsPrimary

UNION ALL 

-- Cit pointer for INDI and FAM events
SELECT 
  EW.Waymarks || ' ' || S.Name AS Waymarks
  , C.*  
FROM CitationTable C
JOIN SourceTable S
USING(SourceID)
JOIN EventWay EW
ON C.OwnerID = EW.EventID
WHERE C.OwnerType = 2 

UNION ALL 

-- Cit pointer for Alternate Names
SELECT
  NW.Person --|| CHAR(13) 
  || CASE NW.IsPrimary
     WHEN 1 THEN ' (Name)'
     ELSE ' (Alt Name)'
     END
  || ' ' || S.Name AS Waymarks 
  , C.*
FROM CitationTable C
JOIN SourceTable S
USING(SourceID)
JOIN NameWay NW
ON C.OwnerID = NW.NameID
WHERE C.OwnerType = 7 --AND NOT NW.IsPrimary
)
;
--End of CitationWay

--RoleWay
DROP VIEW IF EXISTS RoleWay
;
CREATE TEMP VIEW RoleWay
AS
SELECT
  'Fact Type' || CHAR(13) || FT.Name || CHAR(13) || '  ' || RoleName AS [Waymarks]
  , R.* 
FROM RoleTable R
JOIN FactTypeTable FT
ON EventType = FactTypeID
;

--WitnessWay
DROP VIEW IF EXISTS WitnessWay
;
CREATE TEMP VIEW WitnessWay
AS
SELECT
  NW.Person ||
  CHAR(13) || ' shared ' || EW.Waymarks AS [Waymarks]  
  , W.*
FROM WitnessTable W
JOIN temp.NameWay NW  
ON W.PersonID = NW.OwnerID
JOIN temp.EventWay EW
USING(EventID)
WHERE NW.IsPrimary
;

--AddressPointer
DROP VIEW IF EXISTS AddressWay
;
CREATE TEMP VIEW AddressWay
AS
SELECT
  CASE AddressType  
      WHEN 0 THEN 'Address'      
      WHEN 1 THEN 'Repository'      
      ELSE 'TBD'      
  END || ' List' || CHAR(13) ||
  Name || CHAR(13) || City || ', ' || State AS Waymarks
  , *  
FROM AddressTable
;

--ResearchPointer
DROP VIEW IF EXISTS ResearchWay
;
CREATE TEMP VIEW ResearchWay
AS
SELECT
  CASE TaskType  
      WHEN 0 THEN 'To Do'      
      WHEN 1 THEN 'Correspondence' 
      WHEN 2 THEN 'Research Log'     
      ELSE 'TBD'      
    END || ' (' ||
    CASE OwnerType  
      WHEN 0 THEN 'Person'      
      WHEN 1 THEN 'Family'      
      WHEN 8 THEN 'General'      
      ELSE 'TBD'      
    END  || ')' || CHAR(13) ||   
    Name || CHAR(13) ||
    CASE OwnerType  
      WHEN 0 THEN (SELECT Person FROM temp.NameWay NW WHERE RT.OwnerID = NW.OwnerID AND NW.IsPrimary)      
      WHEN 1 THEN (SELECT Waymarks FROM temp.[FamilyWay] FW WHERE RT.OwnerID = FW.FamilyID)      
      WHEN 8 THEN 'General'      
      ELSE 'TBD'      
    END AS Waymarks
  , *    
FROM ResearchTable RT
;

--ResearchItemPointer (Research Logs)
DROP VIEW IF EXISTS ResearchItemWay
;
CREATE TEMP VIEW ResearchItemWay
AS
SELECT
  'Research Manager' || CHAR(13) || Waymarks AS Waymarks  
  , RIT.*
FROM ResearchItemTable RIT
JOIN temp.ResearchWay RW
ON RIT.LOGID = RW.TaskID
;  

--MultimediaPointer (Media Gallery)
DROP VIEW IF EXISTS MultimediaWay
;
CREATE TEMP VIEW MultimediaWay
AS
SELECT
  'Media Gallery'  || CHAR(13) || 'Filename: ' || MediaFile || CHAR(13) || 'Caption: ' || Caption 
   AS Waymarks   
  , *
FROM MultimediaTable
;

--MediaLinkPointer (MediaLinkTable or MediaTags)
DROP VIEW IF EXISTS MediaLinkWay
;
CREATE TEMP VIEW MediaLinkWay
AS
SELECT
  MC.Waymarks AS List
  , 'Tag ' ||
    CASE OwnerType
    WHEN 0 THEN     
      'person: ' || (SELECT Person FROM temp.NameWay NW WHERE MLT.OwnerID = NW.OwnerID AND NW.IsPrimary)      
    WHEN 1 THEN    
      'couple: ' || (SELECT Waymarks FROM temp.FamilyWay FW WHERE MLT.OwnerID = FW.FamilyID)      
    WHEN 2 THEN    
      'event: ' || (SELECT Waymarks FROM temp.EventWay EW WHERE MLT.OwnerID = EW.EventID) 
    WHEN 3 THEN
      'source: ' || (SELECT Name FROM SourceTable S WHERE MLT.OwnerID = S.SourceID)
    WHEN 4 THEN
      'citation: ' || (SELECT Waymarks FROM temp.CitationWay CW WHERE MLT.OwnerID = CW.CitationID)
    WHEN 5 THEN
      'place: ' || (SELECT Place FROM temp.PlaceWay P WHERE MLT.OwnerID = P.PlaceID)     
    ELSE 'TBD'    
    END AS Tag    
  , MLT.*
FROM MediaLinkTable MLT
JOIN temp.[MultimediaWay] MC
USING(MediaID)
;

--URLpointer  (Web Tags)
DROP VIEW IF EXISTS URLWay
;
CREATE TEMP VIEW URLWay
AS
SELECT
  'Web Tag for:' || CHAR(13) ||
    CASE UT.OwnerType
    WHEN 0 THEN     
      'person: ' || (SELECT Person FROM temp.NameWay NW WHERE UT.OwnerID = NW.OwnerID AND NW.IsPrimary)      
    WHEN 1 THEN    
      'couple: ' || (SELECT Waymarks FROM temp.FamilyWay FW WHERE UT.OwnerID = FW.FamilyID)      
    WHEN 2 THEN    
      'event: ' || (SELECT Waymarks FROM temp.EventWay EW WHERE UT.OwnerID = EW.EventID) 
    WHEN 3 THEN
      'source: ' || (SELECT Name FROM SourceTable S WHERE UT.OwnerID = S.SourceID)
    WHEN 4 THEN
      'citation: ' || (SELECT Waymarks FROM temp.CitationWay CW WHERE UT.OwnerID = CW.CitationID)
    WHEN 5 THEN
      'place: ' || (SELECT Place FROM temp.PlaceWay PW WHERE UT.OwnerID = PW.PlaceID)     
    WHEN 15 THEN
      (SELECT Waymarks FROM temp.ResearchItemWay RIW WHERE UT.OwnerID = ItemID)     
    ELSE 'TBD'    
    END AS Waymarks    
  , *
FROM URLTable UT
;

--LabelPointer  (Group names)
DROP VIEW IF EXISTS LabelWay
;
CREATE TEMP VIEW LabelWay
AS
SELECT
  CASE LabelType  
    WHEN 0 THEN    
      'Group: ' || CHAR(13) || LabelName      
    ELSE 'Label: TBD'    
    END AS Waymarks    
  , *
FROM
LabelTable
;

-- Link Pointer (FSFT)
DROP VIEW IF EXISTS LinkWay
;
CREATE TEMP VIEW LinkWay
AS
SELECT
  CASE extSystem WHEN 1 THEN 'FamilySearch' ELSE 'TBD' END AS extSysNam 
  , CASE LinkType  
    WHEN 0 THEN    
      (SELECT Person || ' (' || extID || ')' FROM temp.NameWay NW WHERE rmID = NW.OwnerID)
    ELSE 'TBD'
    END AS Waymarks    
  , *
FROM LinkTable 
;

-- LinkAncestry Pointer (Ancestry)
DROP VIEW IF EXISTS LinkAncestryWay
;
CREATE TEMP VIEW LinkAncestryWay
AS
SELECT
  CASE extSystem WHEN 2 THEN 'Ancestry' ELSE 'TBD' END AS extSysNam 
  , CASE LinkType  
    WHEN 0 THEN    
      (SELECT Person || ' (' || extID || ')' FROM temp.NameWay NW WHERE rmID = NW.OwnerID)
  	WHEN 4 THEN 
	    (SELECT Waymarks FROM temp.CitationWay CW WHERE rmID = CW.CitationID)
  	WHEN 11 THEN 
	    (SELECT Waymarks FROM temp.MultimediaWay MMW WHERE rmID = MMW.MediaID)
    ELSE 'TBD'
    END AS Waymarks    
  , *
FROM LinkAncestryTable 
;

-- relate the Ancestry media for citations to the citations
DROP TABLE IF EXISTS zMediaCitationTags
;
CREATE TEMP TABLE zMediaCitationTags
AS
SELECT M.MediaID AS MediaID, ML.LinkID AS LinkID, CW.CitationID AS CitationID, LA.extID AS extID, M.MediaFile AS MediaFile, SUBSTR(M.MediaFile,-3)  AS Ext, CW.Waymarks AS WayMarks
FROM LinkAncestryTable LA
JOIN MultiMediaTable M ON LA.rmID = M.MediaID AND LA.LinkType = 11 -- Media
JOIN MediaLinkTable ML USING(MediaID)
JOIN CitationWay CW ON ML.OwnerID = CW.CitationID
WHERE ML.OwnerType = 4 -- Citation
ORDER BY M.MediaID, CW.CitationID
--GROUP BY M.MediaID --, ML.LinkID
--ORDER BY Name, Surname, Given, BirthYear, DeathYear
;

-------------------- Ancestry MediaFile Renaming ----------------

SELECT
'DO NOT CLOSE the SQLite manager. The temp table zMediaCitationTags is needed 
for the next step and can be inspected first for possible anomalies:

STEP 2
Load and execute TreeShare-MediaRenameStep2.sql which repoints the media
links to the new file names and generates a set of Windows Command Line
commands to rename the media files to the new file names.'
AS Instructions

