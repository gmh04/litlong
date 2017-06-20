CREATE TABLE api_author
(
  id         SERIAL PRIMARY KEY,
  forenames  TEXT,
  surname    TEXT NOT NULL,
  gender     CHARACTER(1),
  link       TEXT,
  ol_id      CHARACTER VARYING(16);
  CHECK (surname <> '')
);

CREATE TABLE api_document_author
(
  author_id INTEGER NOT NULL REFERENCES api_author(id),
  document_id INTEGER NOT NULL REFERENCES api_document(id)
);

CREATE TABLE api_publisher
(
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL
);

ALTER TABLE api_document DROP author;
ALTER TABLE api_document ADD publisher_id INTEGER REFERENCES api_publisher(id);
ALTER TABLE api_document ADD active BOOLEAN;
ALTER TABLE api_document ADD ol_id CHARACTER VARYING(16);

CREATE TABLE api_genre
(
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE api_document_genre
(
  genre_id INTEGER NOT NULL REFERENCES api_genre(id),
  document_id INTEGER NOT NULL REFERENCES api_document(id)
);


-- ALTER TABLE api_author ADD ol_id CHARACTER VARYING(16);
