CREATE TABLE api_author
(
  id         SERIAL PRIMARY KEY,
  forenames  TEXT,
  surname    TEXT NOT NULL,
  gender     CHARACTER(1),
  link       TEXT
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

UPDATE api_document SET title = 'Old sights with new eyes' WHERE id = 402;
