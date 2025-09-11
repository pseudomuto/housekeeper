-- Current state: three users
CREATE USER alice IDENTIFIED BY 'pass1';
CREATE USER bob IDENTIFIED BY 'pass2';
CREATE USER charlie IDENTIFIED BY 'pass3';

-- Target state: modify alice, keep bob, drop charlie, add dave
CREATE USER alice IDENTIFIED BY 'newpass1' HOST ANY;
CREATE USER bob IDENTIFIED BY 'pass2';
CREATE USER dave IDENTIFIED BY 'pass4' DEFAULT ROLE admin;
