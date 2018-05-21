-- Auto popolazione DB all'avvio di Spring

-- Se viene lanciata l'eccezione: java.sql.SQLException: Field 'admin_id' doesn't have a default value,
--  cancella tutte le tabelle nel DB e metti @GeneratedValue(strategy = GenerationType.IDENTITY) nell'id dell'entità

INSERT INTO admin (email, username, password) VALUES
    ('email1', 'user1', 'pass1'),
    ('email2', 'user2', 'pass2'),
    ('email3', 'user3', 'pass3');
