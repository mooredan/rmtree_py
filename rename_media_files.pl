#!/usr/bin/perl -w

use DBI;
use strict;

my $sth;
my $driver   = "SQLite"; 
my $database = "../ZebMoore_Ancestry.rmtree";
my $dsn = "DBI:$driver:dbname=$database";
my $userid = "";
my $password = "";
my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1, sqlite_unicode => 1 }) 
   or die $DBI::errstr;

if (! -r "unifuzz.so") {
   printf "ERROR: file unifuzz.so is not readable\n";
   $dbh->disconnect();
   exit(1);
}

my $num_renames = 0;


$dbh->sqlite_enable_load_extension(1);
print "Loading extension\n";
$sth = $dbh->prepare("select load_extension('./unifuzz.so')") or die "Cannot prepare: " . $dbh->errstr();
my $rtn = $sth->execute() or die $DBI::errstr;
print "Extension loaded\n";


print "Opened database $database successfully\n";

# reindex_rmnocase();

my $stmt = qq(SELECT MediaID, MediaPath, MediaFile, MediaType from MultimediaTable;);
$sth = $dbh->prepare( $stmt );
my $rv = $sth->execute() or die $DBI::errstr;


if($rv < 0) {
   print $DBI::errstr;
}

my $row2_count = 0;
my $LinkID;

while(my @row = $sth->fetchrow_array()) {

   # print "MediaID = ". $row[0] . "\n";
   #   print "MediaPath = ". $row[1] ."\n";
   #   print "MediaFile = ". $row[2] ."\n";
   #   next;

   # if ($row[2] =~ /_\./) {   
      # printf("\n");
      # print "MediaID = ". $row[0] . "\n";
      # print "MediaPath = ". $row[1] ."\n";
      # print "MediaFile = ". $row[2] ."\n";

      my $MediaID = $row[0];
      my $MediaFile = $row[2];

      my $relpath = sprintf("%s/%s", "../ZebMoore_Ancestry_media", $MediaFile);
      unless ( -r "$relpath" ) {
         printf "ERROR: MediaID: %d : MediaFile: \"%s\" not readable, going to next match\n", $MediaID, $relpath;
         exit(1);
      }

      my $newMediaFile = $MediaFile;
      $newMediaFile =~ s/ \([0-9,A-F]+\)\././;
      $newMediaFile =~ s/\s\s*/_/g;
      $newMediaFile =~ s/,/_/g;
      $newMediaFile =~ s/\'//g;
      $newMediaFile =~ s/\(/_/g;
      $newMediaFile =~ s/\)/_/g;
      $newMediaFile =~ s/&quot;/_/g;
      $newMediaFile =~ s/&#39;/_/g;
      $newMediaFile =~ s/;_/_/g;
      $newMediaFile =~ s/&/_/g;
      $newMediaFile =~ s/_\././g;
      $newMediaFile =~ s/_-_/-/g;
      $newMediaFile =~ s/__*/_/g;
      $newMediaFile =~ s/^__*//;
      $newMediaFile =~ s/\._//;
      $newMediaFile =~ s|([0-9])\.|${1}_|g;
      $newMediaFile =~ s|([A-Z])\.|${1}_|g;
      $newMediaFile =~ s/_jpg$/.jpg/;
      $newMediaFile =~ s/_html$/.html/;
      $newMediaFile =~ s/_htm$/.htm/;
      $newMediaFile =~ s/_doc$/.doc/;
      $newMediaFile =~ s/_j2k$/.j2k/;
      $newMediaFile =~ s/_pdf$/.pdf/;
      $newMediaFile =~ s/_png$/.png/;
      $newMediaFile =~ s/_txt$/.txt/;

      if ($newMediaFile eq $MediaFile) {
         next;
      }

      # printf "newMediaFile = \"%s\"\n", $newMediaFile;

      my $newrelpath = sprintf("%s/%s", "../ZebMoore_Ancestry_media", $newMediaFile);
      if ( -r "$newrelpath" ) {
         printf "Warning: 1st pass: New file already exists readable\n";

         $newMediaFile = sprintf("%s-%s", $MediaID, $newMediaFile);
         $newrelpath = sprintf("%s/%s", "../ZebMoore_Ancestry_media", $newMediaFile);
         if ( -r "$newrelpath" ) {
            printf "Error: 2nd pass: New file already exists readable\n";
            exit(1);
         }
      }

      printf("rename file: \"%s\" to \"%s\"\n", $MediaFile, $newMediaFile);
      $newrelpath = sprintf("%s/%s", "../ZebMoore_Ancestry_media", $newMediaFile);

      # next;

      # rename file, if success, then update table entry
      my $rtn2 = rename($relpath, $newrelpath);

      if ($rtn2 == 0) {
         printf("ERROR: could not rename \"%s\" to \"%s\"\n", $relpath, $newrelpath);
         exit(1);
      } else {
         printf("INFO: Renamed \"%s\" to \"%s\"\n", $relpath, $newrelpath);

         if ($num_renames == 0) {
            reindex_rmnocase();
         }

         my $stmt2 = sprintf("UPDATE MultiMediaTable SET MediaFile = '%s' WHERE MediaID = %s", $newMediaFile, $MediaID);
         printf "%s ;\n", $stmt2;

         $dbh->do($stmt2) or die $DBI::errstr;
         $num_renames++;
      }
   # }
}

print "INFO: disconnecting\n";
$dbh->disconnect();

printf "INFO: num_renames: %d\n", $num_renames;

exit(0);


sub reindex_rmnocase {
   my $stmt = qq(REINDEX RMNOCASE;);
   printf "%s\n", $stmt;
   my $sth = $dbh->prepare( $stmt );
   my $rv = $sth->execute() or die $DBI::errstr;
   if($rv < 0) {
      print $DBI::errstr;
   }
}
