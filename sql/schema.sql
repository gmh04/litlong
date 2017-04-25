CREATE TABLE api_author
(
  id         SERIAL PRIMARY KEY,
  forenames  TEXT,
  surname    TEXT NOT NULL,
  gender     CHARACTER(1),
  link       TEXT,
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
UPDATE api_document SET title = 'Diary, sketches, and reviews' WHERE id = 192;
UPDATE api_document SET title = $$A woman's first impressions of Europe$$ WHERE id = 198;
UPDATE api_document SET title = 'Christopher North: a memoir of John Wilson' WHERE id = 209;
UPDATE api_document SET title = 'The fortunes of the Falconars vol. 2' WHERE id = 210;
UPDATE api_document SET title = 'Annie Jennings vol. 2' WHERE id = 212;
UPDATE api_document SET title = 'Annie Jennings vol. 1' WHERE id = 211;
UPDATE api_document SET title = 'Colville of the guards vol. 1' WHERE id = 220;
UPDATE api_document SET title = $$The King's own borderers vol. 1$$ WHERE id = 222;
UPDATE api_document SET title = 'The white cockade vol. 2' WHERE id = 219;
UPDATE api_document SET title = 'Memoirs of the life and writings of Thomas Chalmers vol. 2' WHERE id = 230;
UPDATE api_document SET title = 'Memoirs of the life and writings of Thomas Chalmers vol. 4' WHERE id = 231;
UPDATE api_document SET title = '' WHERE id = ;
UPDATE api_document SET title = '' WHERE id = ;
UPDATE api_document SET title = '' WHERE id = ;
UPDATE api_document SET title = '' WHERE id = ;
UPDATE api_document SET title = '' WHERE id = ;
UPDATE api_document SET title = '' WHERE id = ;
