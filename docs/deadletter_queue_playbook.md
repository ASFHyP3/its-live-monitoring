# What to do if there are messages in the DeadLetter Queue?

1. Login to the JPL AWS Console and select the `power_user` role in the `its_live` account
2. Navigate to the SQS -> Queues -> `its-live-monitoring-[test,prod]-DeadLetterQueue...`
3. In the upper-right, click "Send and receive messages"
4. At the bottom, in the "Receive Messages" section, click "Poll for message"
5. For each message: 
   1. copy the scene ID and run its-live-monitoring. For Example:
      ```
      python its_live_monitoring/src/main.py -v [SCENE_ID]
      ```
   2. Fix any issues that arise when running its-live-monitoring
   3. Add a unit or integration test to catch this issue in the future
   4. Delete the message once the issues have been addressed 
6. Open a PR with changes
7. :shipit:
