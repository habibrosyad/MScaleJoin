package mscalejoin.experiment;

/**
 * MScaleJoin.jar [experiment_code] [number_of_threads] [window_size]
 * <p>
 * [experiment_code]:
 * 1 - BandJoinExperiment
 * 2 - EquiJoinCommonNljExperiment
 * 3 - EquiJoinCommonShjExperiment
 * 4 - EquiJoinDistinctNljExperiment
 * 5 - EquiJoinDistinctShjExperiment
 */
public class Experiment {
    public static void main(String[] args) {
        int code, numberOfThreads;
        long windowSize;
        String path = ""; // Dataset path
        AbstractExperiment experiment;

        if (args.length < 4) {
            // Default values
            code = -1;
            numberOfThreads = 2;
            windowSize = 4000000;
        } else {
            code = Integer.parseInt(args[0]);
            numberOfThreads = Integer.parseInt(args[1]);
            windowSize = Long.parseLong(args[2]);
            path = args[3];
        }

        switch (code) {
            case 1:
                experiment = new BandJoinExperiment(windowSize);
                break;
            case 2:
                experiment = new EquiJoinCommonNljExperiment(windowSize);
                break;
            case 3:
                experiment = new EquiJoinCommonShjExperiment(windowSize);
                break;
            case 4:
                experiment = new EquiJoinDistinctNljExperiment(windowSize);
                break;
            case 5:
                experiment = new EquiJoinDistinctShjExperiment(windowSize);
                break;
            default:
                experiment = new EquiJoinCommonShjExperiment(windowSize);
                break;
        }

        experiment.run(numberOfThreads, path);
    }
}
