package org.flink.cdc.mysql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

public class Main {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 9091);
        // configuration.setString("execution.checkpointing.interval", "3min");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment
//                .createLocalEnvironmentWithWebUI(configuration);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        String createSourceTableSQL = "CREATE TABLE source_dest (" +
//                "`id` int," +
//                "`password` string," +
//                "`last_login` string," +
//                "`is_superuser` int," +
//                "`username` string," +
//                "`first_name` string," +
//                "`last_name` string," +
//                "`email` string," +
//                "`is_staff` int," +
//                "`is_active` int," +
//                "`date_joined` string," +
//                "PRIMARY KEY (`id`) NOT ENFORCED" +
//                ") WITH (" +
//                "'connector' = 'mysql-cdc'," +
//                "'hostname' = 'localhost'," +
//                "'username' = 'username'," +
//                "'password' = 'password'," +
//                "'database-name' = 'hue'," +
//                "'table-name' = 'auth_user'," +
//                "'scan.startup.mode' = 'snapshot'" +
//                ");";
//        tableEnv.executeSql(createSourceTableSQL);
//        System.out.println(createSourceTableSQL);
//        tableEnv.executeSql(createSourceTableSQL);
//        tableEnv.executeSql("select  * from source_dest").print();

//        String createSinkTableSQL = "CREATE TABLE sink_dest (" +
//                "`id` INT," +
//                "`name` STRING," +
//                "PRIMARY KEY (`id`) NOT ENFORCED" +
//                ") WITH (" +
//                "'connector' = 'jdbc'," +
//                "'url' = 'jdbc:mysql://10.4.45.207:3306/cdc_test_target'," +
//                "'username' = 'username'," +
//                "'password' = 'password'," +
//                "'table-name' = 'player_target'" +
//                ");";
//        tableEnv.executeSql(createSinkTableSQL);
//
//        String insertSQL = "INSERT INTO sink_dest SELECT * FROM source_dest;";
//        StatementSet statementSet = tableEnv.createStatementSet();
//        statementSet.addInsertSql(insertSQL);
//        statementSet.execute();

//        Properties properties = new Properties();
//        properties.setProperty("snapshot.locking.mode", "none");
//
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("cdc_test") // set captured database
                .tableList("cdc_test.tableA") // set captured table
                .username("root")
                .password("")
//                .serverId("5400-6000") // 设置为区间才能并发运行
                .serverTimeZone("Asia/Shanghai")
                .databaseList("hue")
                .tableList("hue.auth_user")
//                .debeziumProperties(properties)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
//                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.snapshot())
                .build();

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(1)
                .print().setParallelism(1);
        env.execute("Print MySQL Snapshot");
    }
}
