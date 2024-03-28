package com.hudi.flink;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

public class HelloMain {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        
        // 状态后端
        EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        embeddedRocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        env.setStateBackend(embeddedRocksDBStateBackend);
        
        // checkpoint配置
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(30), CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://192.168.100.100:8020/flink/ckps");
        checkpointConfig.setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(20));
        checkpointConfig.setTolerableCheckpointFailureNumber(5);
        checkpointConfig.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(1));
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        tEnv.executeSql("CREATE TABLE source_table (\n" +
                "  uuid varchar(20),\n" +
                "  name varchar(10),\n" +
                "  age int,\n" +
                "  ts timestamp(3),\n" +
                "  `partition` varchar(20)\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1'\n" +
                ")");
        
        tEnv.executeSql("create table t(\n" +
                "  uuid varchar(20),\n" +
                "  name varchar(10),\n" +
                "  age int,\n" +
                "  ts timestamp(3),\n" +
                "  `partition` varchar(20)\n" +
                ")\n" +
                "with (\n" +
                "  'connector' = 'hudi',\n" +
                "  'path' = 'hdfs://192.168.100.100:8020/hudi/hudi_flink/t',\n" +
                "  'table.type' = 'MERGE_ON_READ'\n" +
                ")");
        
        tEnv.executeSql("insert into t select * from source_table");
    }
}
