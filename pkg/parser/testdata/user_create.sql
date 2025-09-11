-- Basic CREATE USER
CREATE USER name1;

-- Basic CREATE USER
CREATE USER `special-user`;

-- with if not exists
CREATE USER IF NOT EXISTS alice;

-- with or replace
CREATE USER OR REPLACE alice2;

-- with if not exists on cluster
CREATE USER IF NOT EXISTS alice ON CLUSTER production;

-- not identified
CREATE USER name1 NOT IDENTIFIED;

-- identified with plaintext password
CREATE USER name2 IDENTIFIED WITH plaintext_password BY 'my_password';

-- identification method omitted
CREATE USER name3 IDENTIFIED BY 'qwerty';

-- identified with sha256_hash password
CREATE USER name4 IDENTIFIED WITH sha256_hash BY '0C268556C1680BEF0640AAC1E7187566704208398DA31F03D18C74F5C5BE5053' SALT '4FB16307F5E10048196966DD7E6876AE53DE6A1D1F625488482C75F14A5097C7';
