DROP TABLE IF EXISTS EfCoreTestChild;
DROP TABLE IF EXISTS EfCoreTest;

DROP SEQUENCE IF EXISTS id_seq;
DROP SEQUENCE IF EXISTS id_seq_child;

CREATE SEQUENCE id_seq;

CREATE TABLE EfCoreTest
(
    ID int PRIMARY KEY NOT NULL DEFAULT nextval('id_seq'),
    Name VARCHAR(50),
    CreatedDate date NOT NULL,
    ModifiedDate date NOT NULL
);

CREATE SEQUENCE id_seq_child;

CREATE TABLE EfCoreTestChild
(
    ID int NOT NULL DEFAULT nextval('id_seq_child'),
	EfCoreTestID int NOT NULL references EfCoreTest(ID),
    Name VARCHAR(50),
    CreatedDate date NOT NULL,
    ModifiedDate date NOT NULL,
	PRIMARY KEY(ID, EfCoreTestID)
);