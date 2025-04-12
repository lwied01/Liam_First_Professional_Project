options(repos = c(CRAN = "https://cloud.r-project.org"))
install.packages("RSQLite")
library(RSQLite)

#Part_1 Clean Data
df_Devprop <- read.csv("C:/Users/Liam/Downloads/DeviceProperty.csv")
df_Production <- read.csv("C:/Users/Liam/Downloads/Productionmetric.csv")
df_quality <- read.csv("C:/Users/Liam/Downloads/Quality.csv")

con <- dbConnect(SQLite(), "Production.sqlite")
dbWriteTable(con, "df_Production", df_Production, overwrite = TRUE)

columns <- dbListFields(con, "df_Production")

#Remove extra unplanned stop time column
columns_to_keep <- columns[columns != "unplanned_stop_time.1"]

select_remove_query <- paste("SELECT", paste(columns_to_keep, collapse = ", "), "FROM df_Production")

dbExecute(con, paste("CREATE TABLE df_Production_clean AS", select_remove_query))
dbExecute(con, "DROP TABLE df_Production;")
dbExecute(con, "ALTER TABLE df_Production_clean RENAME TO df_Production;")

# Remove rows where good_count is negative
df_Production <- df_Production[df_Production$good_count >= 0, ]
#Remove all 0 rows
dbExecute(con, "
  DELETE FROM df_Production
  WHERE deviceKey == 0;
")

df_Production <- dbReadTable(con, "df_Production")

#Append Production and Quality data by Prodmetric stream Key
dbWriteTable(con, "df_Quality", df_quality, overwrite = TRUE)

dbExecute(con, "
  CREATE TABLE df_Prod_quality_appendnew AS
  SELECT 
    p.prodmetric_stream_key,
    p.reject_count AS prod_reject_count,
    p.good_count AS prod_good_count,
    q.count AS quality_reject_count
  FROM df_Production p
  JOIN df_Quality q ON p.prodmetric_stream_key = q.prodmetric_stream_key;
")

df_Prod_quality_append <- dbReadTable(con, "df_Prod_quality_appendnew")
df_Production$good_count[
  df_Production$good_count > 0 & df_Production$run_time == 0
] <- 0



#Part_2

#2.1 Calculate total unplanned and planned stop time, get ratio
Ratio_plan_unplan_stoptime <- sum(df_Production$unplanned_stop_time)/sum(df_Production$planned_stop_time)
print(Ratio_plan_unplan_stoptime)

#2.2

# function that extracts downtime per production line

extract_downtime_per_line <- function(line_number){
    line_data <- df_Production[df_Production$deviceKey == line_number, ] #Extract data per line
    downtime <- line_data$unplanned_stop_time + line_data$planned_stop_time  
    return(downtime)
}
#Function that extracts key elements for analysis

key_analysis_ideas_and_plot <- function(line_number){
  downtime <- extract_downtime_per_line(line_number)
  standard_deviation <- sd(downtime)
  median <- median(downtime)
  range <- max((downtime)) - min((downtime))
  mean <- mean(downtime)
  boxplot(downtime,
          main = paste("Downtime for", line_number),
          ylab = "Downtime",
          col = "lightblue")
  return(c("The mean is:", mean,
      "The Standard Deviation is:", standard_deviation,
      "The median is:", median,
      "The range is:", range))
}
#Apply to lines 
line_names <- c("Line1", "Line2", "Line3", "Line4")
key_data <- sapply(line_names, key_analysis_ideas_and_plot)
print(key_data)

# ANOVA to test differences
df_Production$total_downtime <- df_Production$planned_stop_time + df_Production$unplanned_stop_time

anova_downtime <- aov(total_downtime ~ deviceKey, data = df_Production)
summary(anova_downtime)


#2.3 
#Count unplanned stop times greater than 0
unplanned_stop_count <- df_Production[df_Production$unplanned_stop_time > 0, ]

#Get most frequent unplanned stop reason
unplanned_stop_reason <- table(unplanned_stop_count$process_state_reason_display_name)

par(mar = c(10, 4, 4, 2))
barplot(unplanned_stop_reason,
        main = "Reasons For Unplanned Downtimes",
        xlab = "",
        ylab = "Frequency",
        las = 2,
        col = "pink",
        cex.names = 1)
mtext("Reject reason", side = 1, line = 8, adj = .7)

main_reason <- names(which.max(unplanned_stop_reason))

print(paste("The main unplanned stop reason is:", main_reason))

#Part_3

#3.1 - Calculate the overall reject rate (total reject_count / total good_count + reject_count)

#Use appended table data
total_reject_count <- df_Prod_quality_append$prod_reject_count + df_Prod_quality_append$quality_reject_count
overall_reject_rate <- sum(total_reject_count)/ sum(df_Prod_quality_append$prod_good_count + total_reject_count)

print(overall_reject_rate)
#3.2 Most common reject reason
reason_freq <- table(df_quality$reject_reason_display_name)

# Create a barplot using the frequency table
barplot(reason_freq, 
        main = "Reject Reason Frequencies", 
        xlab = "", 
        ylab = "Frequency",
        las = 2,
        col = 'blue')
mtext("Reject reason", side = 1, line = 8)
main_reason_reject <- names(which.max(reason_freq))

print(paste("The main reject reason is:", main_reason_reject))

#3.3 
#Get average production rate
avg_rate_by_devicekey <- function(line_number) {
  new_line_data <- df_Production[df_Production$deviceKey == line_number & df_Production$run_time > 0, ]
  # Total run time in hours
  total_hours_runtime <- sum(new_line_data$run_time, na.rm = TRUE) / 60
  total_good_count <- sum(new_line_data$good_count, na.rm = TRUE)
    avg_rate <- total_good_count / total_hours_runtime
  
  if (!is.finite(avg_rate)) {
    avg_rate <- 0
  }
  
  return(avg_rate)
}

line_numbers <- c("Line1", "Line2", "Line3", "Line4")
avg_rates <- sapply(line_numbers, avg_rate_by_devicekey)
print(avg_rates)
# It seems as though there are noticeable differences, let's run a test to be sure
df_Production$rate_per_hour <- df_Production$good_count / (df_Production$run_time / 60)
df_clean <- df_Production[is.finite(df_Production$rate_per_hour) & df_Production$run_time > 0, ]
anova_result <- aov(rate_per_hour ~ deviceKey, data = df_clean)
summary(anova_result)
print("Because the p-value is 8.75^-8, we can conclude that there is a statistically significant difference in production rates!")



#3.4
# See whether reject count and unplanned stop time are correlated
# Filter rows where both values are positive
df_production_positive <- df_Production[df_Production$reject_count > 0 & df_Production$unplanned_stop_time > 0, ]

#scatterplot
plot(df_production_positive$reject_count, df_production_positive$unplanned_stop_time,
     main = "Scatterplot of Reject Count vs Unplanned Stop Time",
     xlab = "Reject Count", ylab = "Unplanned Stop Time")

#Correlation test for clarification
cor_results <- cor.test(df_production_positive$reject_count, df_production_positive$unplanned_stop_time, method = "pearson")
message <- paste("Based on a p-value of about", cor_results$p.value,
                 ", we can conclude that there is no statistically significant correlation between Unplanned stop time and Production reject counts.")
print(message)

#Part 4 - Compare key metrics (e.g., average downtime per shift, 
#average reject rate per shift) across different shift_display_name or team_display_name. 
#Visualize these comparisons.

avg_downtime_by_team <- function(shift_number){
  
  shift_data <- df_Production[df_Production$team_display_name == shift_number, ] #Extract data by Shift
  avg_downtime <- sum(team_data$unplanned_stop_time + team_data$planned_stop_time) / nrow(shift_data)
  
  return(avg_downtime)
}
avg_downtime_by_shift <- function(shift_number){
  
  shift_data <- df_Production[df_Production$shift_display_name == shift_number, ] # Extract data by shift
  avg_downtime <- sum(shift_data$unplanned_stop_time + shift_data$planned_stop_time) / nrow(shift_data)
  
  return(avg_downtime)
}
shift_names <- c("First Shift", "Second Shift", "Third Shift")

avg_downtimes <- sapply(shift_names, avg_downtime_by_shift)

print(avg_downtimes)

avg_downtime_df <- data.frame(
  Shift = shift_names,
  Avg_Downtime = avg_downtimes
)

barplot(avg_downtime_df$Avg_Downtime,
        names.arg = avg_downtime_df$Shift,
        col = "skyblue",
        main = "Average Downtime by Shift",
        ylab = "Average Downtime (minutes)",
        xlab = "Shift")

#Test for Consistency
df_Production$total_downtime <- df_Production$unplanned_stop_time + df_Production$planned_stop_time
anova_downtime <- aov(total_downtime ~ shift_display_name, data = df_Production)
summary(anova_downtime)


# Average reject rate per shift
avg_reject_by_shift <- function(shift_number){
  
  reject_data <- df_Production[df_Production$shift_display_name == shift_number, ] #Extract data by team
  avg_reject <- sum(reject_data$reject_count) / nrow(reject_data)
  
  return(avg_reject)
}
shift_names <- c("First Shift", "Second Shift", "Third Shift")

avg_rejects <- sapply(shift_names, avg_reject_by_shift)

avg_reject_df <- data.frame(
  Shift = shift_names,
  Avg_reject = avg_rejects
)
barplot(avg_reject_df$Avg_reject,
        names.arg = avg_downtime_df$Shift,
        col = "green",
        main = "Average reject by Shift",
        ylab = "Average rejects",
        xlab = "Shift",
        )
print(avg_rejects)

#Test for consistency
anova_rejects <- aov(reject_count ~ shift_display_name, data = df_Production)
summary(anova_rejects)


