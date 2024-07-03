package edu.coursera.concurrent;

import edu.rice.pcdp.Actor;

import java.util.ArrayList;

import static edu.rice.pcdp.PCDP.finish;

/**
 * An actor-based implementation of the Sieve of Eratosthenes.
 *
 * TODO Fill in the empty SieveActorActor actor class below and use it from
 * countPrimes to determin the number of primes <= limit.
 */
public final class SieveActor extends Sieve {
    /**
     * {@inheritDoc}
     *
     * TODO Use the SieveActorActor class to calculate the number of primes <=
     * limit in parallel. You might consider how you can model the Sieve of
     * Eratosthenes as a pipeline of actors, each corresponding to a single
     * prime number.
     */
    @Override
    public int countPrimes(final int limit) {
        final SieveActorActor sieveActorActor = new SieveActorActor(2);

        finish(() -> {
            for (int i=3; i<limit; i+=2) {
                sieveActorActor.send(i);
            }
            sieveActorActor.send(0);
        });

        int primes = 0;
        SieveActorActor actor = sieveActorActor;
        while (actor != null) {
            primes += actor.numPrimes;
            actor = actor.nextActor;
        }

        return primes;
    }

    /**
     * An actor class that helps implement the Sieve of Eratosthenes in
     * parallel.
     */
    public static final class SieveActorActor extends Actor {
        /**
         * Process a single message sent to this actor.
         *
         * TODO complete this method.
         *
         * @param msg Received message
         */
        private static final int MAX_LOCAL_PRIMES = 1_000;
        private ArrayList<Integer> primesList;
        private int numPrimes;
        private SieveActorActor nextActor;

        //Constructor
        SieveActorActor(final int pLocalPrime) {
            primesList = new ArrayList<>();
            primesList.add(pLocalPrime);
            this.nextActor = null;
            this.numPrimes = 1;
        }

        @Override
        public void process(final Object msg) {
            // Cast the message that enters the actor as an integer
            final int candidate = (Integer) msg;
            if (candidate > 0) {
                if (isLocallyPrime(candidate)) {
                    if (primesList.size() <= MAX_LOCAL_PRIMES) {
                        primesList.add(candidate);
                        numPrimes ++;
                    }
                    else if (nextActor == null) {
                        nextActor = new SieveActorActor(candidate);
                    }
                    else {
                        nextActor.send(msg);
                    }
                }
            }
        }

        private boolean isLocallyPrime(int candidate) {
            for (int i = 0; i < numPrimes; i++) {
                if ((candidate % primesList.get(i)) == 0)
                    return false;
            }
            return true;
        }
    }
}
