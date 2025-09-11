-- Current state: existing user
CREATE USER bob IDENTIFIED BY 'oldpass' HOST LOCAL;

-- Target state: modified user (different password and host)
CREATE USER bob IDENTIFIED BY 'newpass' HOST ANY DEFAULT DATABASE analytics;
