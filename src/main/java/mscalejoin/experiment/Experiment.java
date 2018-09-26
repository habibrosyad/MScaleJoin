package mscalejoin.experiment;

/**
 * MScaleJoin.jar [experiment_code] [number_of_threads] [stream_rate] [window_size]
 */
public class Experiment {
    public static void main(String[] args) {
        int code, numberOfThreads, rate;
        long windowSize;
        String path;
        AbstractExperiment experiment;

        if (args.length < 5) {
            // Default values for basic testing
            code = -1;
            numberOfThreads = 4;
            rate = 1000; // 1000 t/s
            windowSize = 20000; // 20s
            path = "/Users/habib.rosyad/sandbox/MScaleJoin/dataset/shj/1000000/";
        } else {
            code = Integer.parseInt(args[0]);
            numberOfThreads = Integer.parseInt(args[1]);
            rate = Integer.parseInt(args[2]);
            windowSize = Long.parseLong(args[3]);
            path = args[4];
        }

        switch (code) {
            case 0:
                experiment = new Scenario1(windowSize);
                break;
            case 1:
                experiment = new Scenario2(windowSize);
                break;
            case 2:
                experiment = new Scenario3(windowSize);
                break;
            case 3:
                experiment = new Scenario4a1(windowSize);
                break;
            case 4:
                experiment = new Scenario4a2(windowSize);
                break;
            case 5:
                experiment = new Scenario4b1(windowSize);
                break;
            case 6:
                experiment = new Scenario4b2(windowSize);
                break;
            default:
                experiment = new Scenario4a2(windowSize);
                break;
        }

        // Run the experiment
        experiment.setPath(path);
        experiment.setRate(rate);
        experiment.run(numberOfThreads);
    }
}
