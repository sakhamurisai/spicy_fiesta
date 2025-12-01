
-- =====================================================================
-- SECTION 14: SAMPLE STATS & SEED (OPTIONAL)
-- =====================================================================
-- (Omitted - add seed files based on your ETL / environment)

-- =====================================================================
-- END OF MONOLITHIC SCHEMA
-- =====================================================================

-- =====================================================================
-- SECTION 13: INITIAL DATA POPULATION (All 50 US States)
-- =====================================================================

-- Insert All 50 US States
INSERT INTO store.States (StateCode, StateName, StateRegion, TaxRate) VALUES
('AL', 'Alabama', 'South', 0.0400),
('AK', 'Alaska', 'West', 0.0000),
('AZ', 'Arizona', 'West', 0.0560),
('AR', 'Arkansas', 'South', 0.0650),
('CA', 'California', 'West', 0.0725),
('CO', 'Colorado', 'West', 0.0290),
('CT', 'Connecticut', 'Northeast', 0.0635),
('DE', 'Delaware', 'Northeast', 0.0000),
('FL', 'Florida', 'South', 0.0600),
('GA', 'Georgia', 'South', 0.0400),
('HI', 'Hawaii', 'West', 0.0400),
('ID', 'Idaho', 'West', 0.0600),
('IL', 'Illinois', 'Midwest', 0.0625),
('IN', 'Indiana', 'Midwest', 0.0700),
('IA', 'Iowa', 'Midwest', 0.0600),
('KS', 'Kansas', 'Midwest', 0.0650),
('KY', 'Kentucky', 'South', 0.0600),
('LA', 'Louisiana', 'South', 0.0445),
('ME', 'Maine', 'Northeast', 0.0550),
('MD', 'Maryland', 'South', 0.0600),
('MA', 'Massachusetts', 'Northeast', 0.0625),
('MI', 'Michigan', 'Midwest', 0.0600),
('MN', 'Minnesota', 'Midwest', 0.0688),
('MS', 'Mississippi', 'South', 0.0700),
('MO', 'Missouri', 'Midwest', 0.0423),
('MT', 'Montana', 'West', 0.0000),
('NE', 'Nebraska', 'Midwest', 0.0550),
('NV', 'Nevada', 'West', 0.0685),
('NH', 'New Hampshire', 'Northeast', 0.0000),
('NJ', 'New Jersey', 'Northeast', 0.0663),
('NM', 'New Mexico', 'West', 0.0513),
('NY', 'New York', 'Northeast', 0.0400),
('NC', 'North Carolina', 'South', 0.0475),
('ND', 'North Dakota', 'Midwest', 0.0500),
('OH', 'Ohio', 'Midwest', 0.0575),
('OK', 'Oklahoma', 'South', 0.0450),
('OR', 'Oregon', 'West', 0.0000),
('PA', 'Pennsylvania', 'Northeast', 0.0600),
('RI', 'Rhode Island', 'Northeast', 0.0700),
('SC', 'South Carolina', 'South', 0.0600),
('SD', 'South Dakota', 'Midwest', 0.0450),
('TN', 'Tennessee', 'South', 0.0700),
('TX', 'Texas', 'South', 0.0625),
('UT', 'Utah', 'West', 0.0595),
('VT', 'Vermont', 'Northeast', 0.0600),
('VA', 'Virginia', 'South', 0.0530),
('WA', 'Washington', 'West', 0.0650),
('WV', 'West Virginia', 'South', 0.0600),
('WI', 'Wisconsin', 'Midwest', 0.0500),
('WY', 'Wyoming', 'West', 0.0400);
GO

-- Insert Order Channels
INSERT INTO ord.OrderChannels (ChannelCode, ChannelName) VALUES
('INSTORE', 'In-Store'),
('DRIVETHRU', 'Drive-Thru'),
('MOBILE', 'Mobile App'),
('WEB', 'Website'),
('KIOSK', 'Self-Service Kiosk'),
('THIRDPARTY', 'Third Party Delivery');
GO

