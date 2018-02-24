DROP TABLE EfCoreTest;
DROP SEQUENCE id_seq;

CREATE SEQUENCE id_seq;

CREATE TABLE EfCoreTest
(
    ID int PRIMARY KEY NOT NULL DEFAULT nextval('id_seq'),
    Name VARCHAR(50),
    CreatedDate date NOT NULL,
    ModifiedDate date NOT NULL
);