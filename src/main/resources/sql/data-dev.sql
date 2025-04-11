ALTER TABLE providers
MODIFY provider_type ENUM(
  'ARCA',
  'CLIEN',
  'COOLANDJOY',
  'DAMOANG',
  'FMKOREA',
  'PPOMPPU',
  'PPOMPPUEN',
  'QUASAR',
  'RULIWEB',
  'DEALBADA'
) NOT NULL;

INSERT IGNORE INTO providers(provider_type) VALUES ('ARCA');
INSERT IGNORE INTO providers(provider_type) VALUES ('CLIEN');
INSERT IGNORE INTO providers(provider_type) VALUES ('COOLANDJOY');
INSERT IGNORE INTO providers(provider_type) VALUES ('DAMOANG');
INSERT IGNORE INTO providers(provider_type) VALUES ('FMKOREA');
INSERT IGNORE INTO providers(provider_type) VALUES ('PPOMPPU');
INSERT IGNORE INTO providers(provider_type) VALUES ('PPOMPPUEN');
INSERT IGNORE INTO providers(provider_type) VALUES ('QUASAR');
INSERT IGNORE INTO providers(provider_type) VALUES ('RULIWEB');
INSERT IGNORE INTO providers(provider_type) VALUES ('DEALBADA');
