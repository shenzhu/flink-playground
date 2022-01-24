package sql._02_window;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

public class CumulateWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(
                        6,
                        org.apache.flink.api.common.time.Time.of(10L, TimeUnit.MINUTES),
                        org.apache.flink.api.common.time.Time.of(5L, TimeUnit.SECONDS)));
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setParallelism(1);

        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // SQL query
        String sourceSql =
                "CREATE TABLE source_table (\n"
                        + "    user_id STRING,\n"
                        + "    price BIGINT,\n"
                        + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n"
                        + "    WATERMARK FOR row_time AS row_time\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '1',\n"
                        + "  'fields.user_id.length' = '2',\n"
                        + "  'fields.price.min' = '1',\n"
                        + "  'fields.price.max' = '10'\n"
                        + ")";

        String sinkSql =
                "CREATE TABLE sink_table (\n"
                        + "    start_time TIMESTAMP,\n"
                        + "    end_time TIMESTAMP,\n"
                        + "    sum_price DOUBLE\n"
                        + ") WITH (\n"
                        + "  'connector' = 'print'\n"
                        + ")";

        String selectWhereSql =
                "INSERT INTO sink_table\n"
                        + "SELECT window_start, window_end, SUM(price)\n"
                        + "FROM TABLE(CUMULATE(TABLE source_table, DESCRIPTOR(row_time), INTERVAL '2' SECONDS, INTERVAL '10' SECONDS))\n"
                        + "GROUP BY window_start, window_end\n";

        tEnv.getConfig().getConfiguration().setString("pipeline.name", "Cumulate Window Test");

        tEnv.executeSql(sourceSql);
        tEnv.executeSql(sinkSql);
        tEnv.executeSql(selectWhereSql);
    }
}
