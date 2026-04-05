-- =========================================
-- Контрольная работа 3
-- Тема: База данных онлайн-кинотеатра
-- =========================================

-- Подключаем расширения
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;



-- =========================================
-- Создание таблиц
-- =========================================

-- Таблица режиссёров
CREATE TABLE directors (
    director_id BIGSERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name  VARCHAR(100) NOT NULL,
    birth_date DATE,
    country    VARCHAR(100)
);

-- Таблица актёров
CREATE TABLE actors (
    actor_id BIGSERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name  VARCHAR(100) NOT NULL,
    birth_date DATE,
    country    VARCHAR(100)
);

-- Таблица жанров
CREATE TABLE genres (
    genre_id BIGSERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

-- Таблица фильмов
CREATE TABLE movies (
    movie_id BIGSERIAL PRIMARY KEY,
    title VARCHAR(300) NOT NULL,
    release_year INT NOT NULL CHECK (release_year BETWEEN 1900 AND 2100),
    director_id BIGINT NOT NULL REFERENCES directors(director_id),
    rating NUMERIC(3,1),
    description TEXT NOT NULL,
    search_vector tsvector
);

-- Связь многие-ко-многим: фильмы и актёры
CREATE TABLE movie_actors (
    movie_id BIGINT REFERENCES movies(movie_id) ON DELETE CASCADE,
    actor_id BIGINT REFERENCES actors(actor_id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, actor_id)
);

-- Связь многие-ко-многим: фильмы и жанры
CREATE TABLE movie_genres (
    movie_id BIGINT REFERENCES movies(movie_id) ON DELETE CASCADE,
    genre_id BIGINT REFERENCES genres(genre_id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, genre_id)
);

-- Таблица пользователей
CREATE TABLE users (
    user_id BIGSERIAL PRIMARY KEY,
    email VARCHAR(200) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- История просмотров
CREATE TABLE watch_history (
    history_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(user_id),
    movie_id BIGINT REFERENCES movies(movie_id),
    watched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================================
-- Триггер для полнотекстового поиска
-- =========================================

CREATE OR REPLACE FUNCTION movies_search_vector_update()
RETURNS trigger AS
$$
BEGIN
    -- Формируем tsvector из title и description
    NEW.search_vector :=
        setweight(to_tsvector('english', coalesce(NEW.title, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(NEW.description, '')), 'B');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_movies_search
BEFORE INSERT OR UPDATE OF title, description
ON movies
FOR EACH ROW
EXECUTE FUNCTION movies_search_vector_update();

-- =========================================
-- Индексы
-- =========================================

-- Индексы для внешних ключей
CREATE INDEX idx_movies_director ON movies(director_id);
CREATE INDEX idx_movie_actors_actor ON movie_actors(actor_id);
CREATE INDEX idx_movie_genres_genre ON movie_genres(genre_id);
CREATE INDEX idx_watch_history_user ON watch_history(user_id);
CREATE INDEX idx_watch_history_movie ON watch_history(movie_id);

-- GIN-индекс для полнотекстового поиска
CREATE INDEX idx_movies_search_vector
ON movies
USING GIN(search_vector);

-- Триграммные индексы для частичного поиска
CREATE INDEX idx_movies_title_trgm
ON movies USING GIN (title gin_trgm_ops);

CREATE INDEX idx_movies_desc_trgm
ON movies USING GIN (description gin_trgm_ops);

-- =========================================
-- Наполнение данными
-- =========================================

-- Режиссёры
INSERT INTO directors (first_name, last_name, birth_date, country) VALUES
('Christopher', 'Nolan', '1970-07-30', 'UK'),
('Quentin', 'Tarantino', '1963-03-27', 'USA'),
('Steven', 'Spielberg', '1946-12-18', 'USA');

-- Актёры
INSERT INTO actors (first_name, last_name, birth_date, country) VALUES
('Leonardo', 'DiCaprio', '1974-11-11', 'USA'),
('Brad', 'Pitt', '1963-12-18', 'USA'),
('Joseph', 'Gordon-Levitt', '1981-02-17', 'USA'),
('Samuel', 'Jackson', '1948-12-21', 'USA');

-- Жанры
INSERT INTO genres (name) VALUES
('Action'),
('Drama'),
('Sci-Fi'),
('Thriller'),
('Crime');

-- Фильмы
INSERT INTO movies (title, release_year, director_id, rating, description) VALUES
(
 'Inception',
 2010,
 1,
 8.8,
 'A skilled thief enters dreams to steal secrets and must perform inception.'
),
(
 'Interstellar',
 2014,
 1,
 8.6,
 'Explorers travel through space to save humanity.'
),
(
 'Pulp Fiction',
 1994,
 2,
 8.9,
 'Crime stories with violence and dark humor.'
),
(
 'Django Unchained',
 2012,
 2,
 8.4,
 'A freed slave seeks to rescue his wife.'
),
(
 'Jurassic Park',
 1993,
 3,
 8.1,
 'Dinosaurs are brought back to life in a park.'
);

-- Связь фильмов и актёров
INSERT INTO movie_actors VALUES
(1,1),(1,3),
(2,1),
(3,2),(3,4),
(4,2),
(5,4);

-- Связь фильмов и жанров
INSERT INTO movie_genres VALUES
(1,1),(1,3),
(2,3),
(3,5),
(4,1),
(5,3);

-- Пользователи
INSERT INTO users (email, name) VALUES
('john@example.com','John'),
('anna@example.com','Anna');

-- История просмотров
INSERT INTO watch_history (user_id, movie_id) VALUES
(1,1),(1,2),
(2,3),(2,5);

-- Обновляем search_vector
UPDATE movies SET title = title;

-- =========================================
-- Примеры поисковых запросов
-- =========================================

-- Полнотекстовый поиск
EXPLAIN ANALYZE
SELECT title,
       ts_rank(search_vector, websearch_to_tsquery('english', 'dream')) AS rank
FROM movies
WHERE search_vector @@ websearch_to_tsquery('english', 'dream')
ORDER BY rank DESC;

-- Триграммный поиск (по части слова)
EXPLAIN ANALYZE
SELECT title,
       similarity(title, 'interstel') AS sim
FROM movies
WHERE title % 'interstel'
ORDER BY sim DESC;

-- Комбинированный поиск
EXPLAIN ANALYZE
SELECT title,
       ts_rank_cd(search_vector, websearch_to_tsquery('english', 'space')) AS rank,
       similarity(title, 'inter') AS sim
FROM movies
WHERE search_vector @@ websearch_to_tsquery('english', 'space')
   OR title % 'inter'
ORDER BY rank DESC, sim DESC;

-- =========================================
-- ПОЛНОТЕКСТОВЫЙ ПОИСК
-- =========================================

-- Полнотекстовый поиск с логическими операторами
SELECT
    m.movie_id,
    m.title,
    ts_rank_cd(
        m.search_vector,
        to_tsquery('english', 'dream | crime')
    ) AS relevance
FROM movies m
WHERE m.search_vector @@ to_tsquery('english', 'dream | crime')
ORDER BY relevance DESC;

-- Полнотекстовый поиск (websearch)
SELECT
    m.movie_id,
    m.title,
    ts_rank_cd(
        m.search_vector,
        websearch_to_tsquery('english', 'space OR survival')
    ) AS relevance
FROM movies m
WHERE m.search_vector @@ websearch_to_tsquery('english', 'space OR survival')
ORDER BY relevance DESC;

-- =========================================
-- ПОИСК ПО ЧАСТИ СЛОВА (TRIGRAM)
-- =========================================

SELECT
    movie_id,
    title,
    similarity(title, 'interstel') AS sim
FROM movies
WHERE title ILIKE '%interstel%'
   OR title % 'interstel'
ORDER BY sim DESC, title;

-- =========================================
-- КОМБИНИРОВАННЫЙ ПОИСК
-- =========================================

SELECT
    m.movie_id,
    m.title,
    ts_rank_cd(
        m.search_vector,
        websearch_to_tsquery('english', 'space exploration')
    ) AS fts_rank,
    similarity(m.title, 'inter') AS title_sim,
    similarity(m.description, 'space') AS desc_sim,
    (
        ts_rank_cd(
            m.search_vector,
            websearch_to_tsquery('english', 'space exploration')
        ) * 0.7
        + greatest(similarity(m.title, 'inter'), similarity(m.description, 'space')) * 0.3
    ) AS total_rank
FROM movies m
WHERE
    m.search_vector @@ websearch_to_tsquery('english', 'space exploration')
    OR m.title % 'inter'
    OR m.description % 'space'
ORDER BY total_rank DESC, m.title;