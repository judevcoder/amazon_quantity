-- auto-generated definition
CREATE TABLE products
(
  id          INT AUTO_INCREMENT
    PRIMARY KEY,
  name        TEXT                                NOT NULL,
  source_asin VARCHAR(16)                         NOT NULL,
  asin        VARCHAR(16)                         NOT NULL,
  seller      VARCHAR(224) DEFAULT ''             NOT NULL,
  color       VARCHAR(128) DEFAULT '-'            NOT NULL,
  size        VARCHAR(128) DEFAULT ''             NOT NULL,
  price       FLOAT DEFAULT '0'                   NOT NULL,
  bsr         INT DEFAULT '0'                     NOT NULL,
  reviews     INT DEFAULT '0'                     NOT NULL,
  stars       FLOAT DEFAULT '0'                   NOT NULL,
  quantity    INT                                 NOT NULL,
  sold        INT DEFAULT '0'                     NOT NULL,
  updated     TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  url         VARCHAR(256)                        NULL
);
CREATE INDEX asin
  ON products (asin);
CREATE INDEX asin_2
  ON products (asin, seller);
CREATE INDEX seller
  ON products (seller);
