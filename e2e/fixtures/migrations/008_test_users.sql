-- Create test users for E2E testing
CREATE USER test_reader IDENTIFIED BY 'password123';

CREATE USER test_writer
    IDENTIFIED WITH plaintext_password BY 'writer_pass'
    HOST IP '192.168.1.0/24';
