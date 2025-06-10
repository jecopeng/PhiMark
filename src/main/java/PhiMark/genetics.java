package PhiMark;

import io.jenetics.Genotype;
import io.jenetics.IntegerChromosome;
import io.jenetics.IntegerGene;
import io.jenetics.engine.Engine;
import io.jenetics.engine.EvolutionResult;
import io.jenetics.engine.Limits;
import io.jenetics.util.Factory;

import java.util.Vector;

/**
 * Parameter optimization through a genetic algorithm
 */
public class genetics {
    static Data d;
    static int watlen;//The length of watermark
    static int index;//Current attribute index

    /**
     * 2.fitness fuction
     * @param gt
     * @return
     */
    private static Double eval(Genotype<IntegerGene> gt) {
        int i1 = gt.getChromosome(0).getGene().intValue();
        int i2 = gt.getChromosome(1).getGene().intValue();
        return compute_diff(i1,i2);
    }

    /**
     *  Calculate fitness using parameters m and e
     * @param m    partition count
     * @param e    embedding interval
     * @return
     */
    private static double compute_diff(int m,int e){
        double res = Math.abs(d.len[index] - m*e*watlen*Math.log(2));
        return res;
    }

    /**
     * Optimize the parameters of each attribute using genetic algorithms
     * @param data
     * @param len length of watermark
     * @return
     */
    public static Vector<Integer> getGene(Data data, int len){
        d = data;
        watlen = len;
        Vector<Integer> vec = new Vector<>(2 * d.len.length);
        for(int i = 0;i<d.d.size();i++){
            index = i;
            // 1.) Define the genotype (factory) suitable
            //     for the problem.
            Factory<Genotype<IntegerGene>> gtf =
                    Genotype.of(IntegerChromosome.of(1,1000),//range of partition count
                            IntegerChromosome.of(1,100));//range of embedding interval

            // 3.) Create the execution environment.
            Engine<IntegerGene, Double> engine = Engine
                    .builder(genetics::eval, gtf)//The default population size is 50
                    .minimizing()
                    .build();

            // 4.) Start the execution (evolution) and
            //     collect the result.
            Genotype<IntegerGene> result = engine.stream()
                    .limit(Limits.byFitnessThreshold(d.len[i]*0.025))
                    .limit(100000)//the max iterations of
                    .collect(EvolutionResult.toBestGenotype());

            if(eval(result)>d.len[i]*0.025){//The attribute is unsuitable for watermarking, remove the attribute from the watermark attributes.
                System.out.println(i+"The attribute is unsuitable for watermarking");
                d.d.remove(i);
                d.combine.remove(i);
                i--;
                continue;
            }

            //Save the parameters of the watermark attributes:
            //the parameters at index 2*i corresponds to the m of the i-th watermark attribute,
            //the parameters at index 2i+1 corresponds to the e of the i-th watermark attribute
            vec.add(result.getChromosome(0).getGene().intValue());
            vec.add(result.getChromosome(1).getGene().intValue());
        }


        return vec;
    }
}
