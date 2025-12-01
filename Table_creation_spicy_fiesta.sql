-- SPICY FIESTA RESTAURANT FRANCHISE - FINAL MONOLITHIC SCHEMA
-- Generated: 2025-11-21
-- Option A: Full Calendar Dimension Integration (CalendarID FK used for all date columns)

-- =====================================================================
-- SCHEMA CREATION
-- =====================================================================
CREATE SCHEMA dbo;
GO
CREATE SCHEMA emp;
GO
CREATE SCHEMA store;
GO
CREATE SCHEMA menu;
GO
CREATE SCHEMA ord;
GO
CREATE SCHEMA promo;
GO
CREATE SCHEMA inv;
GO
CREATE SCHEMA finance;
GO
CREATE SCHEMA loyalty;
GO
CREATE SCHEMA dim;
GO

-- =====================================================================
-- DIMENSION: Calendar (Master Date Table)
-- =====================================================================
CREATE TABLE dim.Calendar (
    CalendarID INT IDENTITY(1,1) PRIMARY KEY,
    CalendarDate DATE NOT NULL UNIQUE,

    Year INT NOT NULL,
    Quarter TINYINT NOT NULL,
    Month TINYINT NOT NULL,
    MonthName NVARCHAR(20) NOT NULL,
    Day TINYINT NOT NULL,
    DayOfWeek TINYINT NOT NULL,             -- 1 = Monday .. 7 = Sunday
    DayName NVARCHAR(20) NOT NULL,
    WeekOfYear TINYINT NOT NULL,
    ISOYear SMALLINT NOT NULL,
    ISOWeek TINYINT NOT NULL,

    IsWeekend BIT NOT NULL,
    IsHoliday BIT NOT NULL DEFAULT 0,
    HolidayName NVARCHAR(200) NULL,

    IsMonthStart BIT NOT NULL,
    IsMonthEnd BIT NOT NULL,
    IsQuarterStart BIT NOT NULL,
    IsQuarterEnd BIT NOT NULL,
    IsYearStart BIT NOT NULL,
    IsYearEnd BIT NOT NULL,

    FiscalYear SMALLINT NOT NULL,
    FiscalMonth TINYINT NOT NULL,
    FiscalQuarter TINYINT NOT NULL,

    DayOfYear SMALLINT NOT NULL,

    CreatedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE INDEX IX_Calendar_CalendarDate ON dim.Calendar(CalendarDate);
CREATE INDEX IX_Calendar_Year_Month ON dim.Calendar(Year, Month);
CREATE INDEX IX_Calendar_ISOYear_ISOWeek ON dim.Calendar(ISOYear, ISOWeek);
GO

-- =====================================================================
-- SECTION 2: STORE MANAGEMENT SCHEMA (store.*)
-- =====================================================================

-- US States Reference Table
CREATE TABLE store.States (
    StateID INT IDENTITY(1,1) PRIMARY KEY,
    StateCode CHAR(2) NOT NULL UNIQUE,
    StateName NVARCHAR(50) NOT NULL,
    StateRegion NVARCHAR(20) NOT NULL, -- West, Midwest, South, Northeast
    TaxRate DECIMAL(5,4) NOT NULL DEFAULT 0.0,
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

CREATE INDEX IX_States_StateCode ON store.States(StateCode);
CREATE INDEX IX_States_Region ON store.States(StateRegion);
GO

-- Store Locations (5000+ locations)
CREATE TABLE store.Locations (
    LocationID INT IDENTITY(1,1) PRIMARY KEY,
    LocationGUID UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() UNIQUE,
    StoreNumber NVARCHAR(20) NOT NULL UNIQUE, -- SF-CA-0001
    StoreName NVARCHAR(100) NOT NULL,

    -- Address Information
    AddressLine1 NVARCHAR(255) NOT NULL,
    AddressLine2 NVARCHAR(255) NULL,
    City NVARCHAR(100) NOT NULL,
    StateID INT NOT NULL FOREIGN KEY REFERENCES store.States(StateID),
    ZipCode NVARCHAR(10) NOT NULL,
    County NVARCHAR(50) NULL,

    -- Geographic Coordinates
    Latitude DECIMAL(10,8) NULL,
    Longitude DECIMAL(11,8) NULL,

    -- Store Details
    PhoneNumber NVARCHAR(20) NOT NULL,
    Email NVARCHAR(100) NULL,
    StoreType NVARCHAR(20) NOT NULL, -- Standalone, Mall, Airport, DriveThru
    HasDriveThru BIT NOT NULL DEFAULT 1,
    HasDineIn BIT NOT NULL DEFAULT 1,
    SeatingCapacity INT NULL,

    -- Operating Information
    OpeningDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),  -- CHANGED
    ClosingDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),     -- CHANGED
    IsActive BIT NOT NULL DEFAULT 1,
    Is24Hours BIT NOT NULL DEFAULT 0,

    -- Franchise Information
    IsCorporateOwned BIT NOT NULL DEFAULT 0,
    FranchiseOwnerID INT NULL,

    -- Financial
    AnnualRevenue DECIMAL(15,2) NULL,
    MonthlyRent DECIMAL(10,2) NULL,

    -- Audit
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CreatedBy NVARCHAR(100) NOT NULL,
    ModifiedBy NVARCHAR(100) NOT NULL
);
GO

CREATE INDEX IX_Locations_StoreNumber ON store.Locations(StoreNumber);
CREATE INDEX IX_Locations_StateID ON store.Locations(StateID);
CREATE INDEX IX_Locations_ZipCode ON store.Locations(ZipCode);
CREATE INDEX IX_Locations_IsActive ON store.Locations(IsActive) WHERE IsActive = 1;
CREATE INDEX IX_Locations_GeoCoords ON store.Locations(Latitude, Longitude);
GO

-- Store Operating Hours
CREATE TABLE store.OperatingHours (
    OperatingHoursID INT IDENTITY(1,1) PRIMARY KEY,
    LocationID INT NOT NULL FOREIGN KEY REFERENCES store.Locations(LocationID),
    DayOfWeek TINYINT NOT NULL, -- 1=Monday, 7=Sunday
    OpenTime TIME NOT NULL,
    CloseTime TIME NOT NULL,
    IsHoliday BIT NOT NULL DEFAULT 0,
    HolidayName NVARCHAR(50) NULL,
    EffectiveDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),    -- CHANGED
    ExpiryDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),           -- CHANGED
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

CREATE INDEX IX_OperatingHours_LocationID ON store.OperatingHours(LocationID);
GO

-- =====================================================================
-- SECTION 3: EMPLOYEE MANAGEMENT SCHEMA (emp.*)
-- =====================================================================

-- Employee Positions/Roles
CREATE TABLE emp.Positions (
    PositionID INT IDENTITY(1,1) PRIMARY KEY,
    PositionCode NVARCHAR(20) NOT NULL UNIQUE,
    PositionName NVARCHAR(100) NOT NULL,
    PositionLevel TINYINT NOT NULL, -- 1=Entry, 2=Supervisor, 3=Manager, 4=District, 5=Regional, 6=Corporate
    Department NVARCHAR(50) NOT NULL, -- Operations, Kitchen, Front-of-House, Management
    MinHourlyRate DECIMAL(6,2) NOT NULL,
    MaxHourlyRate DECIMAL(6,2) NOT NULL,
    RequiresCertification BIT NOT NULL DEFAULT 0,
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

-- Employees
CREATE TABLE emp.Employees (
    EmployeeID INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeGUID UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() UNIQUE,
    EmployeeNumber NVARCHAR(20) NOT NULL UNIQUE, -- EMP-000001

    -- Personal Information
    FirstName NVARCHAR(50) NOT NULL,
    MiddleName NVARCHAR(50) NULL,
    LastName NVARCHAR(50) NOT NULL,
    DateOfBirth DATE NOT NULL,
    SSN VARBINARY(256) NOT NULL, -- Encrypted
    Gender CHAR(1) NULL, -- M, F, O

    -- Contact Information
    Email NVARCHAR(100) NOT NULL UNIQUE,
    PhoneNumber NVARCHAR(20) NOT NULL,
    AlternatePhone NVARCHAR(20) NULL,
    EmergencyContactName NVARCHAR(100) NOT NULL,
    EmergencyContactPhone NVARCHAR(20) NOT NULL,

    -- Address
    AddressLine1 NVARCHAR(255) NOT NULL,
    AddressLine2 NVARCHAR(255) NULL,
    City NVARCHAR(100) NOT NULL,
    StateID INT NOT NULL FOREIGN KEY REFERENCES store.States(StateID),
    ZipCode NVARCHAR(10) NOT NULL,

    -- Employment Information
    HireDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),          -- CHANGED
    TerminationDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),       -- CHANGED
    EmploymentStatus NVARCHAR(20) NOT NULL, -- Active, OnLeave, Terminated, Suspended
    EmploymentType NVARCHAR(20) NOT NULL, -- FullTime, PartTime, Seasonal, Contractor
    PositionID INT NOT NULL FOREIGN KEY REFERENCES emp.Positions(PositionID),
    PrimaryLocationID INT NOT NULL FOREIGN KEY REFERENCES store.Locations(LocationID),
    SupervisorEmployeeID INT NULL FOREIGN KEY REFERENCES emp.Employees(EmployeeID),

    -- Compensation
    HourlyRate DECIMAL(6,2) NOT NULL,
    OvertimeEligible BIT NOT NULL DEFAULT 1,

    -- Security & Access
    PasswordHash VARBINARY(256) NOT NULL,
    PasswordSalt VARBINARY(256) NOT NULL,
    LastLoginDate DATETIME2 NULL,
    IsSystemAdmin BIT NOT NULL DEFAULT 0,
    IsActive BIT NOT NULL DEFAULT 1,

    -- Audit
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CreatedBy NVARCHAR(100) NOT NULL,
    ModifiedBy NVARCHAR(100) NOT NULL,

    CONSTRAINT CHK_Employees_HourlyRate CHECK (HourlyRate > 0),
    CONSTRAINT CHK_Employees_TerminationDate CHECK (TerminationDateID IS NULL OR TerminationDateID >= HireDateID)
);
GO

CREATE INDEX IX_Employees_EmployeeNumber ON emp.Employees(EmployeeNumber);
CREATE INDEX IX_Employees_Email ON emp.Employees(Email);
CREATE INDEX IX_Employees_LocationID ON emp.Employees(PrimaryLocationID);
CREATE INDEX IX_Employees_PositionID ON emp.Employees(PositionID);
CREATE INDEX IX_Employees_Status ON emp.Employees(EmploymentStatus) WHERE EmploymentStatus = 'Active';
CREATE INDEX IX_Employees_SupervisorID ON emp.Employees(SupervisorEmployeeID);
GO

-- Employee Schedule/Shifts
CREATE TABLE emp.Schedules (
    ScheduleID INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID INT NOT NULL FOREIGN KEY REFERENCES emp.Employees(EmployeeID),
    LocationID INT NOT NULL FOREIGN KEY REFERENCES store.Locations(LocationID),
    ShiftDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),         -- CHANGED
    StartTime TIME NOT NULL,
    EndTime TIME NOT NULL,
    ShiftType NVARCHAR(20) NOT NULL, -- Morning, Afternoon, Evening, Night, Split
    BreakMinutes INT NOT NULL DEFAULT 30,
    IsApproved BIT NOT NULL DEFAULT 0,
    ApprovedBy INT NULL FOREIGN KEY REFERENCES emp.Employees(EmployeeID),
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

CREATE INDEX IX_Schedules_EmployeeID ON emp.Schedules(EmployeeID);
CREATE INDEX IX_Schedules_ShiftDateID ON emp.Schedules(ShiftDateID);
CREATE INDEX IX_Schedules_LocationID ON emp.Schedules(LocationID);
GO

-- Employee Clock In/Out (Time Tracking)
CREATE TABLE emp.TimeTracking (
    TimeTrackingID BIGINT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID INT NOT NULL FOREIGN KEY REFERENCES emp.Employees(EmployeeID),
    LocationID INT NOT NULL FOREIGN KEY REFERENCES store.Locations(LocationID),
    ClockInDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID), -- CHANGED (date for clock in)
    ClockInTime DATETIME2 NOT NULL,
    ClockOutDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),
    ClockOutTime DATETIME2 NULL,
    BreakStartTime DATETIME2 NULL,
    BreakEndTime DATETIME2 NULL,
    TotalHoursWorked AS (DATEDIFF(MINUTE, ClockInTime, ISNULL(ClockOutTime, SYSUTCDATETIME())) / 60.0),
    IsApproved BIT NOT NULL DEFAULT 0,
    ApprovedBy INT NULL FOREIGN KEY REFERENCES emp.Employees(EmployeeID),
    Notes NVARCHAR(500) NULL,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

CREATE INDEX IX_TimeTracking_EmployeeID ON emp.TimeTracking(EmployeeID);
CREATE INDEX IX_TimeTracking_ClockInDateID ON emp.TimeTracking(ClockInDateID);
CREATE INDEX IX_TimeTracking_ClockInTime ON emp.TimeTracking(ClockInTime);
GO

-- =====================================================================
-- SECTION 4: MENU MANAGEMENT SCHEMA (menu.*)
-- =====================================================================

-- Menu Categories
CREATE TABLE menu.Categories (
    CategoryID INT IDENTITY(1,1) PRIMARY KEY,
    CategoryCode NVARCHAR(20) NOT NULL UNIQUE,
    CategoryName NVARCHAR(100) NOT NULL,
    Description NVARCHAR(500) NULL,
    DisplayOrder INT NOT NULL DEFAULT 0,
    ImageUrl NVARCHAR(500) NULL,
    IsActive BIT NOT NULL DEFAULT 1,
    ParentCategoryID INT NULL FOREIGN KEY REFERENCES menu.Categories(CategoryID),
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

-- Menu Items
CREATE TABLE menu.Items (
    ItemID INT IDENTITY(1,1) PRIMARY KEY,
    ItemGUID UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() UNIQUE,
    ItemCode NVARCHAR(20) NOT NULL UNIQUE, -- TACO-001
    ItemName NVARCHAR(100) NOT NULL,
    Description NVARCHAR(1000) NULL,
    CategoryID INT NOT NULL FOREIGN KEY REFERENCES menu.Categories(CategoryID),

    -- Pricing
    BasePrice DECIMAL(8,2) NOT NULL,
    TaxCategory NVARCHAR(20) NOT NULL DEFAULT 'Food', -- Food, Beverage

    -- Nutritional Information
    Calories INT NULL,
    ProteinGrams DECIMAL(5,2) NULL,
    CarbsGrams DECIMAL(5,2) NULL,
    FatGrams DECIMAL(5,2) NULL,
    SodiumMg INT NULL,

    -- Allergen Information
    ContainsDairy BIT NOT NULL DEFAULT 0,
    ContainsGluten BIT NOT NULL DEFAULT 0,
    ContainsNuts BIT NOT NULL DEFAULT 0,
    ContainsSoy BIT NOT NULL DEFAULT 0,
    ContainsEggs BIT NOT NULL DEFAULT 0,
    IsVegetarian BIT NOT NULL DEFAULT 0,
    IsVegan BIT NOT NULL DEFAULT 0,
    IsSpicy BIT NOT NULL DEFAULT 0,
    SpiceLevel TINYINT NULL, -- 1=Mild, 2=Medium, 3=Hot, 4=ExtraHot

    -- Operational
    PrepTimeMinutes INT NOT NULL DEFAULT 5,
    IsAvailableBreakfast BIT NOT NULL DEFAULT 0,
    IsAvailableLunch BIT NOT NULL DEFAULT 1,
    IsAvailableDinner BIT NOT NULL DEFAULT 1,
    IsAvailableLateNight BIT NOT NULL DEFAULT 1,
    IsFeatured BIT NOT NULL DEFAULT 0,
    IsNewItem BIT NOT NULL DEFAULT 0,
    ImageUrl NVARCHAR(500) NULL,

    -- Status
    IsActive BIT NOT NULL DEFAULT 1,
    LaunchDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),        -- CHANGED
    DiscontinuedDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),      -- CHANGED

    -- Audit
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CreatedBy NVARCHAR(100) NOT NULL,
    ModifiedBy NVARCHAR(100) NOT NULL,

    CONSTRAINT CHK_Items_BasePrice CHECK (BasePrice >= 0)
);
GO

CREATE INDEX IX_Items_ItemCode ON menu.Items(ItemCode);
CREATE INDEX IX_Items_CategoryID ON menu.Items(CategoryID);
CREATE INDEX IX_Items_IsActive ON menu.Items(IsActive) WHERE IsActive = 1;
CREATE INDEX IX_Items_IsFeatured ON menu.Items(IsFeatured) WHERE IsFeatured = 1;
GO

-- Menu Item Customization Options (Modifiers)
CREATE TABLE menu.CustomizationOptions (
    CustomizationOptionID INT IDENTITY(1,1) PRIMARY KEY,
    OptionName NVARCHAR(100) NOT NULL,
    OptionType NVARCHAR(50) NOT NULL, -- Protein, Cheese, Sauce, Topping, Side, Size
    PriceAdjustment DECIMAL(6,2) NOT NULL DEFAULT 0,
    CalorieAdjustment INT NOT NULL DEFAULT 0,
    IsDefault BIT NOT NULL DEFAULT 0,
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

-- Item-Customization Mapping
CREATE TABLE menu.ItemCustomizations (
    ItemCustomizationID INT IDENTITY(1,1) PRIMARY KEY,
    ItemID INT NOT NULL FOREIGN KEY REFERENCES menu.Items(ItemID),
    CustomizationOptionID INT NOT NULL FOREIGN KEY REFERENCES menu.CustomizationOptions(CustomizationOptionID),
    IsRequired BIT NOT NULL DEFAULT 0,
    MinSelection INT NOT NULL DEFAULT 0,
    MaxSelection INT NOT NULL DEFAULT 1,
    DisplayOrder INT NOT NULL DEFAULT 0,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

CREATE INDEX IX_ItemCustomizations_ItemID ON menu.ItemCustomizations(ItemID);
GO

-- Combo Meals
CREATE TABLE menu.ComboMeals (
    ComboID INT IDENTITY(1,1) PRIMARY KEY,
    ComboGUID UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() UNIQUE,
    ComboCode NVARCHAR(20) NOT NULL UNIQUE,
    ComboName NVARCHAR(100) NOT NULL,
    Description NVARCHAR(500) NULL,
    ComboPrice DECIMAL(8,2) NOT NULL,
    SavingsAmount DECIMAL(6,2) NOT NULL DEFAULT 0,
    ImageUrl NVARCHAR(500) NULL,
    IsActive BIT NOT NULL DEFAULT 1,
    LaunchDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),
    ExpiryDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

-- Combo Meal Items
CREATE TABLE menu.ComboItems (
    ComboItemID INT IDENTITY(1,1) PRIMARY KEY,
    ComboID INT NOT NULL FOREIGN KEY REFERENCES menu.ComboMeals(ComboID),
    ItemID INT NOT NULL FOREIGN KEY REFERENCES menu.Items(ItemID),
    Quantity INT NOT NULL DEFAULT 1,
    IsRequired BIT NOT NULL DEFAULT 1,
    AllowSubstitution BIT NOT NULL DEFAULT 0,
    DisplayOrder INT NOT NULL DEFAULT 0
);
GO

CREATE INDEX IX_ComboItems_ComboID ON menu.ComboItems(ComboID);
GO

-- =====================================================================
-- SECTION 5: PROMOTIONS SCHEMA (promo.*)
-- =====================================================================

-- Promotions
CREATE TABLE promo.Promotions (
    PromotionID INT IDENTITY(1,1) PRIMARY KEY,
    PromotionGUID UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() UNIQUE,
    PromotionCode NVARCHAR(50) NOT NULL UNIQUE,
    PromotionName NVARCHAR(200) NOT NULL,
    Description NVARCHAR(1000) NULL,

    -- Promotion Details
    PromotionType NVARCHAR(50) NOT NULL, -- Percentage, FixedAmount, BOGO, FreeItem, ComboDiscount
    DiscountPercentage DECIMAL(5,2) NULL,
    DiscountAmount DECIMAL(8,2) NULL,

    -- Validity (CHANGED -> CalendarID FKs)
    StartDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),         -- CHANGED
    EndDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),           -- CHANGED

    -- Usage Limits
    MaxUsagePerCustomer INT NULL,
    MaxTotalUsage INT NULL,
    CurrentUsageCount INT NOT NULL DEFAULT 0,
    MinimumPurchaseAmount DECIMAL(8,2) NULL,

    -- Applicability
    ApplicableChannels NVARCHAR(100) NOT NULL, -- All, InStore, Online, DriveThru, Mobile
    ApplicableDays NVARCHAR(50) NULL, -- All, Weekdays, Weekends
    ApplicableTimeStart TIME NULL,
    ApplicableTimeEnd TIME NULL,

    -- Status
    IsActive BIT NOT NULL DEFAULT 1,
    IsStackable BIT NOT NULL DEFAULT 0,
    RequiresLoyaltyMembership BIT NOT NULL DEFAULT 0,

    -- Marketing
    ImageUrl NVARCHAR(500) NULL,
    TermsAndConditions NVARCHAR(MAX) NULL,

    -- Audit
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CreatedBy NVARCHAR(100) NOT NULL,

    CONSTRAINT CHK_Promotions_EndDate CHECK (EndDateID > StartDateID),
    CONSTRAINT CHK_Promotions_Discount CHECK (
        (DiscountPercentage IS NULL OR (DiscountPercentage > 0 AND DiscountPercentage <= 100)) AND
        (DiscountAmount IS NULL OR DiscountAmount >= 0)
    )
);
GO

CREATE INDEX IX_Promotions_PromotionCode ON promo.Promotions(PromotionCode);
CREATE INDEX IX_Promotions_Dates ON promo.Promotions(StartDateID, EndDateID);
CREATE INDEX IX_Promotions_IsActive ON promo.Promotions(IsActive) WHERE IsActive = 1;
GO

-- Promotion Item Mapping (Which items are included in promotion)
CREATE TABLE promo.PromotionItems (
    PromotionItemID INT IDENTITY(1,1) PRIMARY KEY,
    PromotionID INT NOT NULL FOREIGN KEY REFERENCES promo.Promotions(PromotionID),
    ItemID INT NULL FOREIGN KEY REFERENCES menu.Items(ItemID),
    CategoryID INT NULL FOREIGN KEY REFERENCES menu.Categories(CategoryID),
    ComboID INT NULL FOREIGN KEY REFERENCES menu.ComboMeals(ComboID),
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),

    CONSTRAINT CHK_PromotionItems_OneReference CHECK (
        (ItemID IS NOT NULL AND CategoryID IS NULL AND ComboID IS NULL) OR
        (ItemID IS NULL AND CategoryID IS NOT NULL AND ComboID IS NULL) OR
        (ItemID IS NULL AND CategoryID IS NULL AND ComboID IS NOT NULL)
    )
);
GO

-- Promotion Store Mapping (Which stores offer this promotion)
CREATE TABLE promo.PromotionStores (
    PromotionStoreID INT IDENTITY(1,1) PRIMARY KEY,
    PromotionID INT NOT NULL FOREIGN KEY REFERENCES promo.Promotions(PromotionID),
    LocationID INT NOT NULL FOREIGN KEY REFERENCES store.Locations(LocationID),
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

CREATE INDEX IX_PromotionStores_PromotionID ON promo.PromotionStores(PromotionID);
CREATE INDEX IX_PromotionStores_LocationID ON promo.PromotionStores(LocationID);
GO

-- =====================================================================
-- SECTION 6: CUSTOMER LOYALTY SCHEMA (loyalty.*)
-- =====================================================================

-- Loyalty Members
CREATE TABLE loyalty.Members (
    MemberID INT IDENTITY(1,1) PRIMARY KEY,
    MemberGUID UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() UNIQUE,
    MemberNumber NVARCHAR(20) NOT NULL UNIQUE,

    -- Personal Information
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    Email NVARCHAR(100) NOT NULL UNIQUE,
    PhoneNumber NVARCHAR(20) NOT NULL,
    DateOfBirth DATE NULL,

    -- Address
    AddressLine1 NVARCHAR(255) NULL,
    City NVARCHAR(100) NULL,
    StateID INT NULL FOREIGN KEY REFERENCES store.States(StateID),
    ZipCode NVARCHAR(10) NULL,

    -- Loyalty Details
    EnrollmentDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),    -- CHANGED
    MembershipTier NVARCHAR(20) NOT NULL DEFAULT 'Bronze', -- Bronze, Silver, Gold, Platinum
    TotalPoints INT NOT NULL DEFAULT 0,
    LifetimePoints INT NOT NULL DEFAULT 0,
    TotalSpent DECIMAL(12,2) NOT NULL DEFAULT 0,

    -- Preferences
    FavoriteLocationID INT NULL FOREIGN KEY REFERENCES store.Locations(LocationID),
    PreferredContactMethod NVARCHAR(20) NOT NULL DEFAULT 'Email', -- Email, SMS, Push
    MarketingOptIn BIT NOT NULL DEFAULT 1,

    -- Security
    PasswordHash VARBINARY(256) NOT NULL,
    PasswordSalt VARBINARY(256) NOT NULL,
    LastLoginDate DATETIME2 NULL,

    -- Status
    IsActive BIT NOT NULL DEFAULT 1,
    DeactivationDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),      -- CHANGED
    DeactivationReason NVARCHAR(200) NULL,

    -- Audit
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

CREATE INDEX IX_Members_Email ON loyalty.Members(Email);
CREATE INDEX IX_Members_PhoneNumber ON loyalty.Members(PhoneNumber);
CREATE INDEX IX_Members_MembershipTier ON loyalty.Members(MembershipTier);
GO

-- Loyalty Points Transactions
CREATE TABLE loyalty.PointsTransactions (
    TransactionID BIGINT IDENTITY(1,1) PRIMARY KEY,
    MemberID INT NOT NULL FOREIGN KEY REFERENCES loyalty.Members(MemberID),
    TransactionType NVARCHAR(20) NOT NULL, -- Earned, Redeemed, Expired, Adjusted, Bonus
    Points INT NOT NULL,
    BalanceAfter INT NOT NULL,
    OrderID BIGINT NULL,
    PromotionID INT NULL FOREIGN KEY REFERENCES promo.Promotions(PromotionID),
    Notes NVARCHAR(500) NULL,
    ExpiryDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),
    TransactionDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),
    CreatedBy NVARCHAR(100) NULL
);
GO

CREATE INDEX IX_PointsTransactions_MemberID ON loyalty.PointsTransactions(MemberID);
CREATE INDEX IX_PointsTransactions_TransactionDateID ON loyalty.PointsTransactions(TransactionDateID);
GO

-- Loyalty Rewards Catalog
CREATE TABLE loyalty.Rewards (
    RewardID INT IDENTITY(1,1) PRIMARY KEY,
    RewardCode NVARCHAR(20) NOT NULL UNIQUE,
    RewardName NVARCHAR(100) NOT NULL,
    Description NVARCHAR(500) NULL,
    PointsCost INT NOT NULL,
    RewardType NVARCHAR(50) NOT NULL, -- FreeItem, Discount, Upgrade
    ItemID INT NULL FOREIGN KEY REFERENCES menu.Items(ItemID),
    DiscountAmount DECIMAL(6,2) NULL,
    ImageUrl NVARCHAR(500) NULL,
    IsActive BIT NOT NULL DEFAULT 1,
    StartDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),
    EndDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

-- =====================================================================
-- SECTION 7: ORDERS SCHEMA (ord.*)
-- =====================================================================

-- Order Channels
CREATE TABLE ord.OrderChannels (
    ChannelID INT IDENTITY(1,1) PRIMARY KEY,
    ChannelCode NVARCHAR(20) NOT NULL UNIQUE,
    ChannelName NVARCHAR(50) NOT NULL, -- InStore, DriveThru, Mobile, Web, Kiosk, ThirdParty
    IsActive BIT NOT NULL DEFAULT 1
);
GO

-- Orders (Main Transaction Table)
CREATE TABLE ord.Orders (
    OrderID BIGINT IDENTITY(1,1) PRIMARY KEY,
    OrderGUID UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() UNIQUE,
    OrderNumber NVARCHAR(50) NOT NULL UNIQUE, -- ORD-YYYYMMDD-XXXX

    -- Location & Channel
    LocationID INT NOT NULL FOREIGN KEY REFERENCES store.Locations(LocationID),
    ChannelID INT NOT NULL FOREIGN KEY REFERENCES ord.OrderChannels(ChannelID),

    -- Customer Information
    MemberID INT NULL FOREIGN KEY REFERENCES loyalty.Members(MemberID),
    CustomerName NVARCHAR(100) NULL,
    CustomerPhone NVARCHAR(20) NULL,
    CustomerEmail NVARCHAR(100) NULL,

    -- Order Details (CHANGED -> CalendarID FK)
    OrderDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),  -- CHANGED
    OrderDateTime DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    OrderStatus NVARCHAR(20) NOT NULL, -- Pending, Confirmed, Preparing, Ready, Completed, Cancelled
    OrderType NVARCHAR(20) NOT NULL, -- DineIn, Takeout, DriveThru, Delivery

    -- Pricing
    SubtotalAmount DECIMAL(12,2) NOT NULL,
    TaxAmount DECIMAL(10,2) NOT NULL,
    DiscountAmount DECIMAL(10,2) NOT NULL DEFAULT 0,
    DeliveryFee DECIMAL(8,2) NOT NULL DEFAULT 0,
    TipAmount DECIMAL(8,2) NOT NULL DEFAULT 0,
    TotalAmount DECIMAL(12,2) NOT NULL,

    -- Payment
    PaymentMethod NVARCHAR(30) NOT NULL, -- Cash, CreditCard, DebitCard, GiftCard, MobileWallet
    PaymentStatus NVARCHAR(20) NOT NULL, -- Pending, Paid, Failed, Refunded
    PaymentDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID), -- CHANGED

    -- Fulfillment
    PreparedBy INT NULL FOREIGN KEY REFERENCES emp.Employees(EmployeeID),
    ProcessedBy INT NULL FOREIGN KEY REFERENCES emp.Employees(EmployeeID),
    EstimatedReadyDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID), -- CHANGED
    ActualReadyDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),    -- CHANGED
    PickupDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),        -- CHANGED

    -- Delivery (if applicable)
    DeliveryAddress NVARCHAR(500) NULL,
    DeliveryInstructions NVARCHAR(1000) NULL,
    DriverID INT NULL FOREIGN KEY REFERENCES emp.Employees(EmployeeID),
    DeliveryDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),       -- CHANGED

    -- Promotions
    PromotionID INT NULL FOREIGN KEY REFERENCES promo.Promotions(PromotionID),
    PromoCode NVARCHAR(50) NULL,
    LoyaltyPointsEarned INT NOT NULL DEFAULT 0,
    LoyaltyPointsRedeemed INT NOT NULL DEFAULT 0,

    -- Special Instructions
    SpecialInstructions NVARCHAR(1000) NULL,

    -- Audit
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),

    CONSTRAINT CHK_Orders_TotalAmount CHECK (TotalAmount >= 0),
    CONSTRAINT CHK_Orders_SubtotalAmount CHECK (SubtotalAmount >= 0)
);
GO

CREATE INDEX IX_Orders_OrderNumber ON ord.Orders(OrderNumber);
CREATE INDEX IX_Orders_OrderDateID ON ord.Orders(OrderDateID);
CREATE INDEX IX_Orders_LocationID ON ord.Orders(LocationID);
CREATE INDEX IX_Orders_MemberID ON ord.Orders(MemberID);
CREATE INDEX IX_Orders_OrderStatus ON ord.Orders(OrderStatus);
CREATE INDEX IX_Orders_PaymentStatus ON ord.Orders(PaymentStatus);
GO

-- Order Items (Line Items)
CREATE TABLE ord.OrderItems (
    OrderItemID BIGINT IDENTITY(1,1) PRIMARY KEY,
    OrderID BIGINT NOT NULL FOREIGN KEY REFERENCES ord.Orders(OrderID),
    ItemID INT NULL FOREIGN KEY REFERENCES menu.Items(ItemID),
    ComboID INT NULL FOREIGN KEY REFERENCES menu.ComboMeals(ComboID),
    ItemName NVARCHAR(200) NOT NULL, -- Snapshot at order time
    Quantity INT NOT NULL DEFAULT 1,
    UnitPrice DECIMAL(10,2) NOT NULL,
    LineTotal DECIMAL(12,2) NOT NULL,
    SpecialInstructions NVARCHAR(500) NULL,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),

    CONSTRAINT CHK_OrderItems_Quantity CHECK (Quantity > 0),
    CONSTRAINT CHK_OrderItems_OneReference CHECK (
        (ItemID IS NOT NULL AND ComboID IS NULL) OR
        (ItemID IS NULL AND ComboID IS NOT NULL)
    )
);
GO

CREATE INDEX IX_OrderItems_OrderID ON ord.OrderItems(OrderID);
CREATE INDEX IX_OrderItems_ItemID ON ord.OrderItems(ItemID);
GO

-- Order Item Customizations
CREATE TABLE ord.OrderItemCustomizations (
    OrderItemCustomizationID BIGINT IDENTITY(1,1) PRIMARY KEY,
    OrderItemID BIGINT NOT NULL FOREIGN KEY REFERENCES ord.OrderItems(OrderItemID),
    CustomizationOptionID INT NOT NULL FOREIGN KEY REFERENCES menu.CustomizationOptions(CustomizationOptionID),
    CustomizationName NVARCHAR(100) NOT NULL, -- Snapshot
    PriceAdjustment DECIMAL(6,2) NOT NULL DEFAULT 0,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

CREATE INDEX IX_OrderItemCustomizations_OrderItemID ON ord.OrderItemCustomizations(OrderItemID);
GO

-- Payment Transactions (Detailed)
CREATE TABLE ord.PaymentTransactions (
    PaymentTransactionID BIGINT IDENTITY(1,1) PRIMARY KEY,
    TransactionGUID UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() UNIQUE,
    OrderID BIGINT NOT NULL FOREIGN KEY REFERENCES ord.Orders(OrderID),

    -- Payment Details
    PaymentDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID), -- CHANGED
    PaymentTime TIME NOT NULL,
    Amount DECIMAL(12,2) NOT NULL,
    TransactionType NVARCHAR(20) NOT NULL, -- Payment, Refund, Void
    TransactionStatus NVARCHAR(20) NOT NULL, -- Pending, Success, Failed, Cancelled

    -- Card Information (Encrypted/Tokenized)
    CardType NVARCHAR(20) NULL,
    Last4Digits NCHAR(4) NULL,
    CardToken NVARCHAR(100) NULL,

    -- Processing Information
    ProcessorName NVARCHAR(50) NULL,
    ProcessorTransactionID NVARCHAR(100) NULL,
    AuthorizationCode NVARCHAR(50) NULL,

    -- Settlement
    SettlementDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),

    -- Additional
    ProcessedBy INT NULL FOREIGN KEY REFERENCES emp.Employees(EmployeeID),
    Notes NVARCHAR(500) NULL,

    TransactionDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),

    CONSTRAINT CHK_PaymentTransactions_Amount CHECK (Amount >= 0)
);
GO

CREATE INDEX IX_PaymentTransactions_OrderID ON ord.PaymentTransactions(OrderID);
CREATE INDEX IX_PaymentTransactions_PaymentDateID ON ord.PaymentTransactions(PaymentDateID);
GO

-- Order Feedback/Reviews
CREATE TABLE ord.OrderFeedback (
    FeedbackID BIGINT IDENTITY(1,1) PRIMARY KEY,
    OrderID BIGINT NOT NULL FOREIGN KEY REFERENCES ord.Orders(OrderID),
    MemberID INT NULL FOREIGN KEY REFERENCES loyalty.Members(MemberID),

    -- Ratings (1-5 scale)
    OverallRating TINYINT NOT NULL,
    FoodQualityRating TINYINT NULL,
    ServiceRating TINYINT NULL,
    SpeedRating TINYINT NULL,
    CleanlinessRating TINYINT NULL,
    ValueRating TINYINT NULL,

    -- Comments
    Comments NVARCHAR(2000) NULL,

    -- Response
    StaffResponse NVARCHAR(2000) NULL,
    RespondedBy INT NULL FOREIGN KEY REFERENCES emp.Employees(EmployeeID),
    ResponseDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID), -- CHANGED

    -- Status
    IsPublic BIT NOT NULL DEFAULT 0,
    IsResolved BIT NOT NULL DEFAULT 0,

    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),

    CONSTRAINT CHK_OrderFeedback_OverallRating CHECK (OverallRating BETWEEN 1 AND 5)
);
GO

CREATE INDEX IX_OrderFeedback_OrderID ON ord.OrderFeedback(OrderID);
CREATE INDEX IX_OrderFeedback_MemberID ON ord.OrderFeedback(MemberID);
CREATE INDEX IX_OrderFeedback_OverallRating ON ord.OrderFeedback(OverallRating);
GO

-- =====================================================================
-- SECTION 8: INVENTORY MANAGEMENT SCHEMA (inv.*)
-- =====================================================================

-- Inventory Items (Raw Materials & Supplies)
CREATE TABLE inv.Items (
    InventoryItemID INT IDENTITY(1,1) PRIMARY KEY,
    ItemCode NVARCHAR(50) NOT NULL UNIQUE,
    ItemName NVARCHAR(200) NOT NULL,
    Description NVARCHAR(1000) NULL,
    Category NVARCHAR(50) NOT NULL, -- Food, Beverage, Packaging, Cleaning, Equipment
    UnitOfMeasure NVARCHAR(20) NOT NULL, -- lb, oz, gal, ea, case

    -- Cost Information
    UnitCost DECIMAL(12,4) NOT NULL,
    ReorderLevel DECIMAL(10,2) NOT NULL,
    ReorderQuantity DECIMAL(10,2) NOT NULL,

    -- Supplier Information
    PrimarySupplierID INT NULL,
    AlternateSupplierID INT NULL,

    -- Storage
    RequiresRefrigeration BIT NOT NULL DEFAULT 0,
    RequiresFreezer BIT NOT NULL DEFAULT 0,
    ShelfLifeDays INT NULL,

    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

CREATE INDEX IX_InvItems_ItemCode ON inv.Items(ItemCode);
CREATE INDEX IX_InvItems_Category ON inv.Items(Category);
GO

-- Store Inventory (Current Stock Levels)
CREATE TABLE inv.StoreInventory (
    StoreInventoryID BIGINT IDENTITY(1,1) PRIMARY KEY,
    LocationID INT NOT NULL FOREIGN KEY REFERENCES store.Locations(LocationID),
    InventoryItemID INT NOT NULL FOREIGN KEY REFERENCES inv.Items(InventoryItemID),

    QuantityOnHand DECIMAL(14,4) NOT NULL DEFAULT 0,
    MinimumQuantity DECIMAL(14,4) NOT NULL,
    MaximumQuantity DECIMAL(14,4) NOT NULL,

    LastRestockDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID), -- CHANGED
    LastCountDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),   -- CHANGED

    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),

    CONSTRAINT UQ_StoreInventory_Location_Item UNIQUE (LocationID, InventoryItemID)
);
GO

CREATE INDEX IX_StoreInventory_LocationID ON inv.StoreInventory(LocationID);
CREATE INDEX IX_StoreInventory_InventoryItemID ON inv.StoreInventory(InventoryItemID);
GO

-- Inventory Transactions (Stock Movements) - replaced earlier with expanded version
CREATE TABLE inv.InventoryTransactions (
    TransactionID BIGINT IDENTITY(1,1) PRIMARY KEY,
    StoreInventoryID BIGINT NOT NULL FOREIGN KEY REFERENCES inv.StoreInventory(StoreInventoryID),
    TransactionType NVARCHAR(20) NOT NULL, -- Receipt, Usage, Adjustment, Waste, Transfer
    Quantity DECIMAL(14,4) NOT NULL,
    QuantityBefore DECIMAL(14,4) NOT NULL,
    QuantityAfter DECIMAL(14,4) NOT NULL,

    -- Additional Details
    OrderID BIGINT NULL FOREIGN KEY REFERENCES ord.Orders(OrderID),
    ReasonCode NVARCHAR(50) NULL,
    Notes NVARCHAR(1000) NULL,

    TransactionDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID), -- CHANGED
    ProcessedBy INT NOT NULL FOREIGN KEY REFERENCES emp.Employees(EmployeeID),
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

CREATE INDEX IX_InventoryTransactions_StoreInventoryID ON inv.InventoryTransactions(StoreInventoryID);
CREATE INDEX IX_InventoryTransactions_TransactionDateID ON inv.InventoryTransactions(TransactionDateID);
GO

-- Purchase Orders
CREATE TABLE inv.PurchaseOrders (
    PurchaseOrderID BIGINT IDENTITY(1,1) PRIMARY KEY,
    PurchaseOrderNumber NVARCHAR(50) NOT NULL UNIQUE,
    LocationID INT NOT NULL FOREIGN KEY REFERENCES store.Locations(LocationID),

    OrderDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID), -- CHANGED
    ExpectedDeliveryDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),
    ActualDeliveryDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),

    OrderStatus NVARCHAR(20) NOT NULL, -- Draft, Submitted, Approved, Shipped, Received, Cancelled

    TotalAmount DECIMAL(14,2) NOT NULL,

    OrderedBy INT NOT NULL FOREIGN KEY REFERENCES emp.Employees(EmployeeID),
    ApprovedBy INT NULL FOREIGN KEY REFERENCES emp.Employees(EmployeeID),
    ReceivedBy INT NULL FOREIGN KEY REFERENCES emp.Employees(EmployeeID),

    Notes NVARCHAR(1000) NULL,

    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

CREATE INDEX IX_PurchaseOrders_LocationID ON inv.PurchaseOrders(LocationID);
CREATE INDEX IX_PurchaseOrders_OrderDateID ON inv.PurchaseOrders(OrderDateID);
GO

-- Purchase Order Items
CREATE TABLE inv.PurchaseOrderItems (
    PurchaseOrderItemID BIGINT IDENTITY(1,1) PRIMARY KEY,
    PurchaseOrderID BIGINT NOT NULL FOREIGN KEY REFERENCES inv.PurchaseOrders(PurchaseOrderID),
    InventoryItemID INT NOT NULL FOREIGN KEY REFERENCES inv.Items(InventoryItemID),

    QuantityOrdered DECIMAL(14,4) NOT NULL,
    QuantityReceived DECIMAL(14,4) NOT NULL DEFAULT 0,
    UnitPrice DECIMAL(12,4) NOT NULL,
    LineTotal DECIMAL(14,2) NOT NULL,

    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

CREATE INDEX IX_PurchaseOrderItems_PurchaseOrderID ON inv.PurchaseOrderItems(PurchaseOrderID);
GO

-- =====================================================================
-- SECTION 9: FINANCIAL SCHEMA (finance.*)
-- =====================================================================

-- Daily Sales Summary
CREATE TABLE finance.DailySalesSummary (
    SummaryID BIGINT IDENTITY(1,1) PRIMARY KEY,
    LocationID INT NOT NULL FOREIGN KEY REFERENCES store.Locations(LocationID),
    SalesDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID), -- CHANGED

    -- Sales Metrics
    TotalOrders INT NOT NULL DEFAULT 0,
    TotalCustomers INT NOT NULL DEFAULT 0,

    -- Revenue Breakdown
    GrossSales DECIMAL(14,2) NOT NULL DEFAULT 0,
    Discounts DECIMAL(12,2) NOT NULL DEFAULT 0,
    Refunds DECIMAL(12,2) NOT NULL DEFAULT 0,
    NetSales DECIMAL(14,2) NOT NULL DEFAULT 0,
    TaxCollected DECIMAL(12,2) NOT NULL DEFAULT 0,

    -- Channel Breakdown
    InStoreSales DECIMAL(12,2) NOT NULL DEFAULT 0,
    DriveThruSales DECIMAL(12,2) NOT NULL DEFAULT 0,
    OnlineSales DECIMAL(12,2) NOT NULL DEFAULT 0,
    DeliverySales DECIMAL(12,2) NOT NULL DEFAULT 0,

    -- Payment Methods
    CashPayments DECIMAL(12,2) NOT NULL DEFAULT 0,
    CardPayments DECIMAL(12,2) NOT NULL DEFAULT 0,
    MobilePayments DECIMAL(12,2) NOT NULL DEFAULT 0,

    -- Labor
    TotalLaborHours DECIMAL(10,2) NOT NULL DEFAULT 0,
    TotalLaborCost DECIMAL(12,2) NOT NULL DEFAULT 0,

    -- Inventory
    CostOfGoodsSold DECIMAL(14,2) NOT NULL DEFAULT 0,

    -- Metrics
    AverageOrderValue DECIMAL(12,2) NOT NULL DEFAULT 0,
    AverageItemsPerOrder DECIMAL(8,2) NOT NULL DEFAULT 0,

    -- Status
    IsReconciled BIT NOT NULL DEFAULT 0,
    ReconciledBy INT NULL FOREIGN KEY REFERENCES emp.Employees(EmployeeID),
    ReconciledDateID INT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),

    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),

    CONSTRAINT UQ_DailySalesSummary UNIQUE (LocationID, SalesDateID)
);
GO

CREATE INDEX IX_DailySalesSummary_LocationID ON finance.DailySalesSummary(LocationID);
CREATE INDEX IX_DailySalesSummary_SalesDateID ON finance.DailySalesSummary(SalesDateID);
GO

-- Store Expenses
CREATE TABLE finance.StoreExpenses (
    ExpenseID BIGINT IDENTITY(1,1) PRIMARY KEY,
    LocationID INT NOT NULL FOREIGN KEY REFERENCES store.Locations(LocationID),

    ExpenseDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID), -- CHANGED
    ExpenseCategory NVARCHAR(50) NOT NULL, -- Rent, Utilities, Repairs, Marketing, Supplies, Insurance
    ExpenseDescription NVARCHAR(1000) NOT NULL,
    Amount DECIMAL(14,2) NOT NULL,

    VendorName NVARCHAR(200) NULL,
    InvoiceNumber NVARCHAR(100) NULL,
    PaymentMethod NVARCHAR(50) NULL,

    ApprovedBy INT NULL FOREIGN KEY REFERENCES emp.Employees(EmployeeID),
    RecordedBy INT NOT NULL FOREIGN KEY REFERENCES emp.Employees(EmployeeID),

    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),

    CONSTRAINT CHK_StoreExpenses_Amount CHECK (Amount >= 0)
);
GO

CREATE INDEX IX_StoreExpenses_LocationID ON finance.StoreExpenses(LocationID);
CREATE INDEX IX_StoreExpenses_ExpenseDateID ON finance.StoreExpenses(ExpenseDateID);
GO

-- Financial Ledger (General Purpose)
CREATE TABLE finance.Ledger (
    LedgerID BIGINT IDENTITY(1,1) PRIMARY KEY,

    LedgerDateID INT NOT NULL FOREIGN KEY REFERENCES dim.Calendar(CalendarID),      -- CHANGED
    LocationID INT NULL FOREIGN KEY REFERENCES store.Locations(LocationID),
    OrderID BIGINT NULL FOREIGN KEY REFERENCES ord.Orders(OrderID),

    EntryType NVARCHAR(50) NOT NULL,        -- Revenue, Tax, Discount, Payment, Refund
    Amount DECIMAL(14,2) NOT NULL,
    GLAccount NVARCHAR(50) NULL,
    Notes NVARCHAR(500) NULL,

    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

CREATE INDEX IX_Ledger_LedgerDateID ON finance.Ledger(LedgerDateID);
CREATE INDEX IX_Ledger_LocationID ON finance.Ledger(LocationID);
GO

-- =====================================================================
-- SECTION 10: SYSTEM/AUDIT TABLES (dbo.*)
-- =====================================================================

-- Audit Log (Track all critical changes)
CREATE TABLE dbo.AuditLog (
    AuditID BIGINT IDENTITY(1,1) PRIMARY KEY,
    TableName NVARCHAR(128) NOT NULL,
    RecordID BIGINT NOT NULL,
    Action NVARCHAR(20) NOT NULL, -- INSERT, UPDATE, DELETE

    OldValues NVARCHAR(MAX) NULL,
    NewValues NVARCHAR(MAX) NULL,

    ChangedBy NVARCHAR(100) NOT NULL,
    ChangedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    IPAddress NVARCHAR(50) NULL,
    ApplicationName NVARCHAR(100) NULL
);
GO

CREATE INDEX IX_AuditLog_TableName ON dbo.AuditLog(TableName);
CREATE INDEX IX_AuditLog_ChangedDate ON dbo.AuditLog(ChangedDate);
GO

-- System Configuration
CREATE TABLE dbo.SystemConfiguration (
    ConfigID INT IDENTITY(1,1) PRIMARY KEY,
    ConfigKey NVARCHAR(100) NOT NULL UNIQUE,
    ConfigValue NVARCHAR(MAX) NOT NULL,
    Description NVARCHAR(500) NULL,
    DataType NVARCHAR(20) NOT NULL DEFAULT 'String',
    IsEncrypted BIT NOT NULL DEFAULT 0,
    ModifiedBy NVARCHAR(100) NOT NULL,
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

-- Error Log
CREATE TABLE dbo.ErrorLog (
    ErrorID BIGINT IDENTITY(1,1) PRIMARY KEY,
    ErrorNumber INT NULL,
    ErrorSeverity INT NULL,
    ErrorState INT NULL,
    ErrorProcedure NVARCHAR(128) NULL,
    ErrorLine INT NULL,
    ErrorMessage NVARCHAR(MAX) NOT NULL,

    UserName NVARCHAR(100) NULL,
    HostName NVARCHAR(100) NULL,
    ApplicationName NVARCHAR(100) NULL,

    ErrorDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

CREATE INDEX IX_ErrorLog_ErrorDate ON dbo.ErrorLog(ErrorDate);
GO

-- =====================================================================
-- SECTION 11: CROSS-SCHEMA CONSTRAINTS & ADJUSTMENTS
-- =====================================================================

-- Add FK from Locations to Employees (Franchise Owner) if exists
ALTER TABLE store.Locations 
ADD CONSTRAINT FK_Locations_FranchiseOwner 
FOREIGN KEY (FranchiseOwnerID) REFERENCES emp.Employees(EmployeeID);
GO

-- Add FK from PointsTransactions to Orders
ALTER TABLE loyalty.PointsTransactions 
ADD CONSTRAINT FK_PointsTransactions_Orders 
FOREIGN KEY (OrderID) REFERENCES ord.Orders(OrderID);
GO

-- =====================================================================
-- SECTION 12: INDEXING, PARTITIONING & AZURE SQL BEST PRACTICES (COMMENTS)
-- =====================================================================

/*
Indexing & Performance Recommendations (Azure SQL):

1. Clustered Indexes:
   - Keep clustered index on identity keys for OLTP: Orders(OrderID), OrderItems(OrderItemID) etc.

2. Nonclustered Indexes:
   - Create composite nonclustered indexes for common query patterns, e.g. (LocationID, OrderDateID), (OrderDateID, OrderStatus)
   - Use filtered indexes for "IsActive = 1" or other low-cardinality flags.

3. Partitioning:
   - For very large fact tables (ord.Orders, ord.OrderItems, ord.PaymentTransactions, inv.InventoryTransactions, finance.Ledger), consider partitioning by LedgerDateID/OrderDateID on Calendar ranges (monthly/quarterly/year).
   - Azure SQL Managed Instance supports partitioning. For single database, use partition schemes and functions.

4. Columnstore:
   - Consider Clustered Columnstore Indexes for large analytical tables (DailySalesSummary, Ledger) to improve compression and analytics queries.

5. Maintenance:
   - Regularly rebuild/reorganize indexes, update statistics.

6. Security:
   - Use TDE (Transparent Data Encryption) on Azure SQL.
   - Store sensitive fields (SSN) encrypted using Always Encrypted or column-level encryption.

7. Naming Conventions:
   - Use schema.table and prefix indexes with IX_, constraints with FK_/CHK_/UQ_.

8. Batch Inserts:
   - Use minimal logging (where possible) and batch sizes for ETL loads.
*/

-- =====================================================================
-- SECTION 13: MERMAID ERD (text) - Paste into a mermaid renderer
-- =====================================================================

-- Mermaid ERD (simplified):
-- Copy the following block (without the SQL comment markers) into a mermaid renderer that supports erDiagram

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

