# ADD
INSERT INTO dedp_schema.events (visit_id, event_time, user_id, page) VALUES
('visit3', NOW() + '1 second', 'user 1', 'home.html'),
('visit3', NOW() + '2 seconds', 'user 1', 'articles.html'),
('visit3', NOW() + '3 seconds', 'user 1', 'contact.html'),

('visit4', NOW() + '2 seconds', 'user 2', 'home.html'),
('visit4', NOW() + '3 seconds', 'user 2', 'about.html'),
('visit4', NOW() + '4 seconds', 'user 2', 'contact.html'),

('visit5', NOW() + '2 seconds', 'user 2', 'home.html'),
('visit5', NOW() + '3 seconds', 'user 2', 'about.html'),
('visit5', NOW() + '4 seconds', 'user 2', 'contact.html');
  
  
 # UPDATE
 UPDATE dedp_schema.events SET page = 'about-me.html' WHERE page = 'about.html';
 
 # DELETE
 DELETE FROM dedp_schema.events WHERE visit_id = 'visit3';
