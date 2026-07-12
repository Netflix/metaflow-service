# Running Flows in Local UI

1. Start your metadata services using the following command:

    ```bash
    ./local-example/run-services.sh
    ```
   
2. Configure metaflow to use your local metadata services:

    ```bash
    metaflow configure import local-example/metaflow-config.json
    ```
3. 

4. Run the example flow:

    ```bash
    python local-example/helloworld.py run
    ```
5. Go to your UI metaflow UI at http://localhost:3000/ to see your flow runs.

![](../../../../Downloads/metaflow UI.png)