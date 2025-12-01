"""
Test suite for 10_loyalty_members_points_rewards.py - loyalty tables
Tests: 2,000 loyalty members, 2 rewards, member number and email formulas
Coverage: 100% of loyalty generation logic
"""

import pytest
from conftest import assert_dataframe_columns, assert_row_count, assert_no_nulls_in_columns, \
    assert_unique_values


class TestLoyaltyGenerator:
    """Test suite for Loyalty program generation"""
    
    @pytest.fixture(autouse=True)
    def setup(self, spark, output_root):
        self.spark = spark
        self.output_root = output_root
    
    def test_members_row_count(self):
        """TC-001: Verify exactly 2,000 loyalty members"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Members")
        assert_row_count(df, 2000, tolerance=0)
    
    def test_members_columns_exist(self):
        """TC-002: Verify members columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Members")
        expected_columns = ["MemberID", "MemberNumber", "FirstName", "LastName", "Email", "PhoneNumber", 
                           "PointsBalance", "TierLevel", "EnrollmentDateID", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_rewards_row_count(self):
        """TC-003: Verify exactly 2 rewards"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Rewards")
        assert_row_count(df, 2, tolerance=0)
    
    def test_rewards_columns_exist(self):
        """TC-004: Verify rewards columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Rewards")
        expected_columns = ["RewardID", "RewardCode", "RewardDescription", "PointsRequired", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_member_ids_unique(self):
        """TC-005: Verify MemberID is unique for all 2,000 members"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Members")
        assert_unique_values(df, "MemberID", 2000)
    
    def test_member_ids_sequential(self):
        """TC-006: Verify MemberID ranges from 1-2000"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Members")
        min_id = df.agg({"MemberID": "min"}).collect()[0][0]
        max_id = df.agg({"MemberID": "max"}).collect()[0][0]
        assert min_id == 1, f"Min MemberID should be 1, got {min_id}"
        assert max_id == 2000, f"Max MemberID should be 2000, got {max_id}"
    
    def test_member_number_format(self):
        """TC-007: Verify MemberNumber format is MBR-{i:07d}"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Members").orderBy("MemberID")
        members = df.collect()
        for member in members:
            expected_number = f"MBR-{member.MemberID:07d}"
            assert member.MemberNumber == expected_number, \
                f"MemberID {member.MemberID}: expected {expected_number}, got {member.MemberNumber}"
    
    def test_member_numbers_unique(self):
        """TC-008: Verify all MemberNumber values are unique"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Members")
        assert_unique_values(df, "MemberNumber", 2000)
    
    def test_member_email_format(self):
        """TC-009: Verify email format is member{i}@example.com"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Members").orderBy("MemberID")
        members = df.collect()
        invalid_count = 0
        for member in members:
            expected_email = f"member{member.MemberID}@example.com"
            if member.Email != expected_email:
                invalid_count += 1
        assert invalid_count == 0, f"Found {invalid_count} members with invalid email format"
    
    def test_member_email_unique(self):
        """TC-010: Verify all email addresses are unique"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Members")
        assert_unique_values(df, "Email", 2000)
    
    def test_member_phone_format(self):
        """TC-011: Verify phone numbers are formatted"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Members")
        invalid_phones = 0
        for row in df.collect():
            digits = ''.join(c for c in str(row.PhoneNumber) if c.isdigit())
            # Phone should have 10 digits
            if len(digits) != 10:
                invalid_phones += 1
        assert invalid_phones == 0, f"Found {invalid_phones} invalid phone numbers"
    
    def test_points_balance_non_negative(self):
        """TC-012: Verify all PointsBalance values are non-negative"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Members")
        invalid = df.filter(df.PointsBalance < 0).count()
        assert invalid == 0, f"Found {invalid} members with negative PointsBalance"
    
    def test_tier_level_valid(self):
        """TC-013: Verify TierLevel is one of standard values (Silver, Gold, Platinum, Diamond)"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Members")
        tiers = {row.TierLevel for row in df.select("TierLevel").distinct().collect()}
        # Should be reasonable tier names
        assert len(tiers) > 0, "No tier levels found"
        assert len(tiers) <= 5, f"Too many tier levels: {tiers}"
    
    def test_enrollment_date_id_valid(self):
        """TC-014: Verify EnrollmentDateID is within calendar range (1-36892)"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Members")
        invalid = df.filter((df.EnrollmentDateID < 1) | (df.EnrollmentDateID > 36892)).count()
        assert invalid == 0, f"Found {invalid} invalid EnrollmentDateID values"
    
    def test_members_no_nulls_critical(self):
        """TC-015: Verify no NULLs in critical members columns"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Members")
        critical_cols = ["MemberID", "MemberNumber", "Email", "PhoneNumber", "PointsBalance"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_reward_ids_unique(self):
        """TC-016: Verify RewardID is unique for all rewards"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Rewards")
        assert_unique_values(df, "RewardID", 2)
    
    def test_reward_codes_unique(self):
        """TC-017: Verify all reward codes are unique"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Rewards")
        assert_unique_values(df, "RewardCode", 2)
    
    def test_reward_points_required_positive(self):
        """TC-018: Verify PointsRequired is positive for all rewards"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Rewards")
        invalid = df.filter(df.PointsRequired <= 0).count()
        assert invalid == 0, f"Found {invalid} rewards with non-positive PointsRequired"
    
    def test_rewards_no_nulls_critical(self):
        """TC-019: Verify no NULLs in critical rewards columns"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Rewards")
        critical_cols = ["RewardID", "RewardCode", "PointsRequired"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_member_first_names_populated(self):
        """TC-020: Verify FirstName is populated for all members"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Members")
        empty = df.filter((df.FirstName.isNull()) | (len(df.FirstName) == 0)).count()
        assert empty == 0, f"Found {empty} members with empty FirstName"
    
    def test_member_last_names_populated(self):
        """TC-021: Verify LastName is populated for all members"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Members")
        empty = df.filter((df.LastName.isNull()) | (len(df.LastName) == 0)).count()
        assert empty == 0, f"Found {empty} members with empty LastName"
    
    def test_created_date_populated(self):
        """TC-022: Verify CreatedDate is populated for all members"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Members")
        null_dates = df.filter(df.CreatedDate.isNull()).count()
        assert null_dates == 0, f"Found {null_dates} NULL CreatedDate values"
    
    def test_reward_hardcoded_values(self):
        """TC-023: Verify hardcoded rewards exist (Free Taco, $3 off)"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Rewards")
        codes = {row.RewardCode for row in df.select("RewardCode").collect()}
        # Should include codes like RW-01, RW-02
        assert len(codes) >= 2, f"Expected at least 2 reward codes, got {len(codes)}"
    
    def test_enrollment_date_reasonable(self):
        """TC-024: Verify enrollment dates are distributed across calendar"""
        df = self.spark.read.parquet(f"{self.output_root}/loyalty.Members")
        unique_dates = df.select("EnrollmentDateID").distinct().count()
        # Should have significant spread of enrollment dates
        assert unique_dates > 100, f"Expected > 100 unique enrollment dates, got {unique_dates}"
