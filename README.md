subscription-dispatcher
=======================

To customize logging, add the following lines to $STORM_HOME/logback/cluster.xml
```
  <appender name="SERVIOTICY" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>${storm.log.dir}/servioticy.log</file>
    <rollingPolicy class="ch.qos.logback.core.rolling.FixedWindowRollingPolicy">
      <fileNamePattern>${storm.log.dir}/logs/servioticy.log.%i</fileNamePattern>
      <minIndex>1</minIndex>
      <maxIndex>9</maxIndex>
    </rollingPolicy>

    <triggeringPolicy class="ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy">
      <maxFileSize>20MB</maxFileSize>
    </triggeringPolicy>

    <encoder>
      <pattern>%d %-8r %m%n</pattern>
    </encoder>
  </appender>

  <logger name="com.servioticy.dispatcher" additivity="false" >
    <level value="INFO"/>
    <appender-ref ref="SERVIOTICY"/>
  </logger>
```
