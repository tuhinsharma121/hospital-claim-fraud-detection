# HospitalClaim Fraud Profiler

## Build the jar

```bash
cd dataAndAnalytics/profiler
sbt clean assembly
cd ~
```

## To run the profiler (note down the pid)
```bash
cd flink-1.8.0
nohup ./bin/flink run -c com.hospitalclaim.fraud.profilers.Profiler /root/dataAndAnalytics/profiler/target/scala-2.11/hospital-claim-fraud-profiler-assembly-0.1.0-SNAPSHOT.jar --config /root/dataAndAnalytics/profiler/src/main/resources/hospital-claim-fraud-profiler.properties > /root/nohup_flink_profiler.out&
```

## To stop the profiler
```bash
kill -9 $pid
```




