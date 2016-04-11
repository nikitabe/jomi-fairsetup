USE [FairSetup20160410]
GO
/****** Object:  StoredProcedure [dbo].[F_GenerateUserCache_Data]    Script Date: 4/11/2016 1:31:24 PM ******/
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

	DECLARE @t_disp TABLE
	(
		[Date] date,
		v float,
		P_Net float,
		direction float
	)

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
		[t] int,
		[PerformanceNet] float
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
		DECLARE @P_net float = 1  -- performance coefficient
		DECLARE @direction float
		-- CURSOR BASED LOOP
		DECLARE @d date, @L float, @P float, @T float, @core_m float, @late_m float, @T_s int
		
		-- Variables to calculate potential and net performance
		DECLARE  @goal_ip	float -- impact potential goal value (Where are we trying to get to?)
				,@step_ip	float -- how fast to we move given performance (How fast will we get there?)
				,@goal_P	float -- goal for performance without level (Generally this is 1 - At Expectation)
				,@step_P	float -- how fast to we move given performance

		DECLARE @last_positive_L float = 0

		DECLARE CUR Cursor FOR SELECT Date, Level, Performance, Throttle, Core_Multiplier, Late_Multiplier, T_to_saturation from @temp_t_work
		OPEN CUR
		FETCH NEXT FROM CUR INTO @d, @L, @P, @T, @core_m, @late_m, @T_s	
		WHILE @@FETCH_STATUS = 0
		

		BEGIN
			-- calculate impact potential
			if @P = 0 SET @P = NULL
			SET @direction = ABS( ISNULL(@p, 1 )-1 )
			
			IF @direction = 0 SET @direction = 1
			ELSE SET @direction = @direction / ( ISNULL(@p, 1 )-1 )

			-- Calculate the step adjuster.  If we are going up, the speed is fast.  If we are going down, the speed is flipped
			DECLARE @S float = ISNULL( @P, 1. )
			IF @S > 1 SET @S = @S
			ELSE IF @S < 1 SET @S = 1-@S

			SET @goal_ip = (@L * ISNULL( @core_m, 1) * ISNULL( @late_m, 1) ) * ISNULL( @P, 1 )
			SET @step_ip = @L / @T_s * @S * @direction

			SET @goal_P = 1. * ISNULL( @P, 1 )
			SET @step_P = 1. / @T_s * @S * @direction


			-- We went from having a level to not having a level
			IF @L > 0 SET @last_positive_L = @L
			ELSE      SET @step_ip = @last_positive_L / @T_s

			-- Move the impact in the right direction
			SET @i_p	= @i_p + @step_ip
			SET @P_net  = @P_net + @step_P

			insert into @t_disp (Date, v, P_net, direction) VALUES (@d, @step_P, @P_net, @direction)

			-- Check if impact is out of bounds
			IF @i_p <= 0								SET @i_p = 0
			ELSE IF @direction > 0 AND @i_p > @goal_ip	SET @i_p = @goal_ip

			IF @P_net <= 0								SET @P_net = 0
			ELSE IF @direction > 0 AND @P_net > @goal_P SET @P_net = @goal_P

			insert into @t_disp (Date, v, P_net, direction) VALUES (@d, @step_P, @P_net, @direction)

			update @temp_t_work set Impact_Potential = @i_p, [internal_step] = @step_ip, PerformanceNet = @P_net where CURRENT OF CUR
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
			   (UserID,   CompanyID,   EventTime, Impact_Potential,   Impact_Actual,   Impact_Actual_LongCycle,   Impact_Actual_LongCycle_RiskAdjusted,   Impact_Money,   Impact_Money_RiskAdjusted,  Impact_Net,                Level, Throttle, Performance, PerformanceLong,     risk_multiplier, PerformanceNet ) 
		(SELECT @user_id, @company_id, [Date],   [Impact_Potential], [Impact_Actual], [Impact_Actual_LongCycle], [Impact_Actual_LongCycle_RiskAdjusted], [Impact_Money], [Impact_Money_RiskAdjusted], ISNULL( [Impact_Net], 0 ), Level, Throttle, Performance, LongCycleMultiplier, risk_multiplier, PerformanceNet FROM @temp_t_work)

	--select * from @temp_t_work

	select * from @t_disp where Date < '2/10/2016' and Date > '2/6/2016'

END
