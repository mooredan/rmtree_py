-- Collection of queries useful with RootsMagic 4 .rmgc databases
-- This set can be copied to a file and saved with the .SQL extension,
-- loaded into SQLiteman, and executed one query at a time by
-- placing the cursor in the selected SQL command line.--
-- The query can be stored as a VIEW in the database - delete the--    semi-colon at the end of the preceding CREATE VIEW line and re-execute.
--
-- All of these queries have been tested with SQLiteman.
-- Use at your own risk.
-- TomH 28 Dec 2009--
-- rev 29 Dec 2009 added VIEWS and requisite COLLATE NOCASE options
-------------------------------------------------------
-- List Persons with Blank Names in the Address List-- (a fault that may occur in a GEDCOM import). -- Drop the last NOT to see all other persons with addresses.
-- TomH 14 Dec 2009
CREATE VIEW blankname_in_addresslist AS; 
SELECT Surname||', '||Given COLLATE NOCASE AS Person, Adr.Name COLLATE NOCASE AS AddressName,
	Adr.Street1, Adr.Street2, Adr.City, Adr.State, Adr.Country, Adr.Phone1, Adr.Fax, Adr.Email
	FROM NameTable AS Nam, AddressLinkTable AS Lnk, AddressTable AS Adr
	WHERE Nam.OwnerID = Lnk.OwnerID AND Lnk.AddressID = Adr.AddressID
	AND Adr.AddressType = 0 AND Adr.Name NOT LIKE '%_';
-------------------------------------------------------
-- List Persons with specified Surnames. 
-- Example of creating a SQL View or Virtual Table and the 
-- explicit use of COLLATE NOCASE to override the RMNOCASE collation
-- defined for certain fields and embedded in the RootsMagic application.
-- TomH 28 Dec 2009
CREATE VIEW selected_surnames AS;
	SELECT Surname||', '||Given COLLATE NOCASE AS Name, BirthYear, DeathYear  
	FROM nametable  
	WHERE surname COLLATE NOCASE IN ('surname1','surname2')  
	ORDER BY surname||given COLLATE NOCASE;
-- replace the surname criterion with one appropriate to your needs
-- and change the View name accordingly.
---------------------------------------------------------
-- Lists Places having Place Details 
-- Romer/kbens0n/TomH 19 Dec 2009
CREATE VIEW PlacesDetails AS; 
SELECT PlaceTable2.Name COLLATE NOCASE AS Place, PlaceTable1.Name COLLATE NOCASE AS PlaceDetails
	FROM PlaceTable AS PlaceTable1, PlaceTable AS PlaceTable2
	WHERE PlaceTable1.MasterID = PlaceTable2.PlaceID
	ORDER by Place||PlaceDetails COLLATE NOCASE;
---------------------------------------------------------
-- List of unused Places 
-- TomH 28 Dec 2009
CREATE VIEW UnusedPlaces AS;
	SELECT PlaceTable.PlaceID, PlaceTable.Name COLLATE NOCASE   
		FROM PlaceTable,       
		(SELECT PlaceTable.PlaceID AS unusedPlaceID          
			FROM PlaceTable          
			EXCEPT SELECT eventtable.PlaceID           
				FROM eventtable)   
	WHERE placetable.PlaceType = 0      
	AND PlaceTable.PlaceID = unusedPlaceID;
-- This query ignores the Places supplied by RootsMagic (PlaceType = 1), 
-- reporting only on those created by the user (PlaceType = 0). 
-- It also skips over Place Details (PlaceType = 2); 
-- as long as Places are added, edited, deleted from within the RootsMagic application, 
-- Place Details should not become orphaned from their parent Places. 
----------------------------------------------------------


