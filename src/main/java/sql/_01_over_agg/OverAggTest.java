package sql._01_over_agg;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

public class OverAggTest {
    public static void main(String[] args) throws Exception {
        // Prepare env
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
                        + "    order_id STRING,\n"
                        + "    price BIGINT,\n"
                        + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n"
                        + "    WATERMARK FOR row_time AS row_time\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '5',\n"
                        + "  'fields.order_id.length' = '2',\n"
                        + "  'fields.price.min' = '1',\n"
                        + "  'fields.price.max' = '1000000'\n"
                        + ")";

        String sinkSql =
                "CREATE TABLE sink_table (\n"
                        + "    order_id STRING,\n"
                        + "    ranking BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'print'\n"
                        + ")";

        String selectWhereSql =
                "insert into sink_table\n"
                        + "select order_id,\n"
                        + "       ROW_NUMBER() over (PARTITION BY order_id ORDER BY row_time) AS ranking\n"
                        + "from source_table\n";

        tEnv.getConfig().getConfiguration().setString("pipeline.name", "Group Agg Test");

        tEnv.executeSql(sourceSql);
        tEnv.executeSql(sinkSql);
        tEnv.executeSql(selectWhereSql);
    }
}
