USE [FairSetup20160410]
GO
/****** Object:  StoredProcedure [dbo].[F_GenerateUserCache_Data]    Script Date: 4/11/2016 12:41:59 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[F_GenerateUserCache_Data]
	@user_id int,
	@company_id int,
	@date_start datetime -- when @date_start is NULL, then generates only new data.  Date at which to start recalculating
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	--DECLARE @company_id int = 10038,
	--		@user_id int = 3--10098

	--exec dbo.D_GenerateUserCache_Segments @user_id, @company_id, '1/1/1900'
	--select * from cache_segments where UserID = @user_id and CompanyID = @company_id

	DECLARE @temp_t_work TABLE
	(
		[Date] date,
		[Level] float,
		[Throttle] float,
		[Performance] float,
		[LongCycleMultiplier] float,
		[Impact_Potential] float,
		[Impact_Actual] float,
		[Impact_Actual_LongCycle] float,
		[Impact_Actual_LongCycle_RiskAdjusted] float,
		[Money_Transfer] float,
		[Impact_Money] float,
		[Impact_Money_RiskAdjusted] float,
		[Impact_Net] float,
		Risk_Multiplier float,
		Core_Multiplier float,
		Late_Multiplier float,
		T_to_saturation int,
		[internal_step] float,
		[t] int
	)

	-- let's get first and last dates
	DECLARE @first_date date, @last_date date, @num_days int
	select top 1 @first_date = EventDate from cache_segments where UserID = @user_id order by EventDate
	SET @last_date = dbo.GetMaxDate( @company_id )
	SET @num_days = DATEDIFF( DAY, @first_date, @last_date)

	INSERT INTO @temp_t_work ( [Date], [Impact_Potential], [Impact_Actual], [t] ) 
		SELECT DATEADD( DAY, Number, @first_date ), -1, 0, Number from DateHelperTable
			where Number between 0 and @num_days   -- changed @start_d to start at 0 and go for @num_days rather than 


	-- pull data out of segment cache that is relevant
	update @temp_t_work
	set 
		t.Level = s.Level, 
		t.Throttle = s.PLevel_Forward, 
		t.Performance = s.PLevel_Backward, 
		t.LongCycleMultiplier = s.LongCycleMultiplier, 
		t.Risk_Multiplier = s.risk_multiplier,
		t.Core_Multiplier = s.core_multiplier,
		t.Late_Multiplier = s.late_multiplier,
		t.T_to_saturation = s.T_to_saturation
	from @temp_t_work t
		left join (select * from cache_segments where UserID = @user_id and CompanyID = @company_id) s on t.Date >= s.EventDate and t.Date <= ISNULL( s.EventDate_Next, @last_date )

	-- note money investments
	update @temp_t_work
	set 
		t.Money_Transfer = s.money_transfer
	from @temp_t_work t
		inner join (select * from user_events where UserID = @user_id and CompanyID = @company_id and money_transfer IS NOT NULL) s on t.Date = s.EventDate
	
	-- Update impact potential
		DECLARE @i_p float = 0
		-- CURSOR BASED LOOP
		DECLARE @d date, @L float, @P float, @T float, @core_m float, @late_m float, @T_s int
		DECLARE @direction int, @goal_L float, @step_s float

		DECLARE @last_positive_L float = 0

		DECLARE CUR Cursor FOR SELECT Date, Level, Performance, Throttle, Core_Multiplier, Late_Multiplier, T_to_saturation from @temp_t_work
		OPEN CUR
		FETCH NEXT FROM CUR INTO @d, @L, @P, @T, @core_m, @late_m, @T_s	
		WHILE @@FETCH_STATUS = 0
		

		BEGIN
			-- calculate impact potential
			if @P = 0 SET @P = NULL
			SET @goal_L = (@L * ISNULL( @core_m, 1) * ISNULL( @late_m, 1) ) * ISNULL( @P, 1 )
			SET @step_s = @goal_L / @T_s

			-- We went from having a level to not having a level
			IF @L > 0 
				SET @last_positive_L = @L
			ELSE
				SET @step_s = @last_positive_L / @T_s

			-- Move the impact in the right direction
			IF		@i_p < @goal_L	SET @i_p = @i_p + @step_s
			ELSE IF @i_p > @goal_L	SET @i_p = @i_p - @step_s

			-- Check if impact is out of bounds
			IF @i_p < 0										SET @i_p = 0

			update @temp_t_work set Impact_Potential = @i_p, [internal_step] = @step_s where CURRENT OF CUR
		FETCH NEXT FROM CUR INTO @d, @L, @P, @T, @core_m, @late_m, @T_s	
		END
		CLOSE CUR
		DEALLOCATE CUR

	-- Calculate 
	-- 1. Actual Impact without Long Cycle Multiplier
	-- 2. Actual Impact with Long Cycle Multiplier
	-- 4. Impact from money

	update @temp_t_work
		SET 
			Impact_Actual			= Impact_Potential * ISNULL( Performance, 1) * ISNULL( Throttle, 0) * ISNULL( Core_Multiplier, 1) * ISNULL( Late_Multiplier,  1),
			Impact_Actual_LongCycle	= Impact_Potential * ISNULL( Performance, 1) * ISNULL( Throttle, 0) * ISNULL( Core_Multiplier, 1) * ISNULL( Late_Multiplier,  1) 
										* ISNULL( LongCycleMultiplier, 1 ),
			Impact_Money			= Money_Transfer / dbo.GetDollarsPerImpactPoint()

	-- Adjust for risk
	update @temp_t_work
		SET 
			Impact_Actual_LongCycle_RiskAdjusted = Impact_Actual_LongCycle * Risk_Multiplier,
			Impact_Money_RiskAdjusted			 = Impact_Money * Risk_Multiplier


	-- calculate total
		DECLARE @t_sum TABLE
		(
			Date date,
			Impact_Net float
		)

		insert into @t_sum (Date, Impact_Net) 
			(select Date, 
				sum( Impact_Actual_LongCycle_RiskAdjusted + ISNULL( Impact_Money_RiskAdjusted, 0) ) over (
					-- partition by UserID
					order by DATE rows unbounded preceding) as Impact_Net from @temp_t_work)
					
	update @temp_t_work
		set Impact_Net = s.Impact_net
			from @temp_t_work t inner join @t_sum s on t.Date = s.Date

	delete from user_impact_cache  where UserID = @user_id and CompanyID = @company_id

	INSERT INTO user_impact_cache 
			   (UserID,   CompanyID,   EventTime, Impact_Potential,   Impact_Actual,   Impact_Actual_LongCycle,   Impact_Actual_LongCycle_RiskAdjusted,   Impact_Money,   Impact_Money_RiskAdjusted,  Impact_Net,                Level, Throttle, Performance, PerformanceLong,     risk_multiplier ) 
		(SELECT @user_id, @company_id, [Date],   [Impact_Potential], [Impact_Actual], [Impact_Actual_LongCycle], [Impact_Actual_LongCycle_RiskAdjusted], [Impact_Money], [Impact_Money_RiskAdjusted], ISNULL( [Impact_Net], 0 ), Level, Throttle, Performance, LongCycleMultiplier, risk_multiplier FROM @temp_t_work)

	--select * from @temp_t_work



END
