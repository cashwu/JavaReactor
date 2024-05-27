package com.cashwu.javareactor;

import org.assertj.core.internal.IterableElementComparisonStrategy;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author cash.wu
 * @since 2024/05/27
 */
public class ReactorTests {

    @Test
    public void createFluxJust() {

        Flux<String> flux = Flux.just("Banana", "Grape", "Apple", "Orange");

        //        flux.subscribe(System.out::println);
        flux.subscribe(f -> System.out.println("fruit : " + f));

        StepVerifier.create(flux).expectNext("Banana").expectNext("Grape").expectNext("Apple")
                    .expectNext("Orange").verifyComplete();
    }

    @Test
    public void createFluxFromArray() {

        String[] fruits = {"Banana", "Grape", "Apple", "Orange"};

        Flux<String> flux = Flux.fromArray(fruits);

        StepVerifier.create(flux).expectNext("Banana").expectNext("Grape").expectNext("Apple")
                    .expectNext("Orange").verifyComplete();
    }

    @Test
    public void createFluxFromIterable() {

        ArrayList<String> fruits = new ArrayList<>();
        fruits.add("Banana");
        fruits.add("Grape");
        fruits.add("Apple");
        fruits.add("Orange");

        Flux<String> flux = Flux.fromIterable(fruits);

        StepVerifier.create(flux).expectNext("Banana").expectNext("Grape").expectNext("Apple")
                    .expectNext("Orange").verifyComplete();
    }

    @Test
    public void createFluxFromStream() {

        Stream<String> fruitSteam = Stream.of("Banana", "Grape", "Apple", "Orange");

        Flux<String> flux = Flux.fromStream(fruitSteam);

        StepVerifier.create(flux).expectNext("Banana").expectNext("Grape").expectNext("Apple")
                    .expectNext("Orange").verifyComplete();
    }

    @Test
    public void createFluxRang() {

        Flux<Integer> flux = Flux.range(1, 5);

        StepVerifier.create(flux).expectNext(1).expectNext(2).expectNext(3).expectNext(4)
                    .expectNext(5).verifyComplete();
    }

    @Test
    public void createFluxInterval() {

        Flux<Long> interalflux = Flux.interval(Duration.ofSeconds(1)).take(5);

        StepVerifier.create(interalflux).expectNext(0L).expectNext(1L).expectNext(2L).expectNext(3L)
                    .expectNext(4L).verifyComplete();
    }

    @Test
    public void mergeFluxes() {

        Flux<String> flux01 = Flux.just("aa", "bb", "cc").delayElements(Duration.ofMillis(500));

        Flux<String> flux02 = Flux.just("11", "22", "33").delaySubscription(Duration.ofMillis(250))
                                  .delayElements(Duration.ofMillis(500));

        Flux<String> mergeFlux = flux01.mergeWith(flux02);
        //        Flux<String> mergedFlux = Flux.merge(flux01, flux02);

        StepVerifier.create(mergeFlux).expectNext("aa").expectNext("11").expectNext("bb")
                    .expectNext("22").expectNext("cc").expectNext("33").verifyComplete();
    }

    @Test
    public void zipFluxes() {

        Flux<String> flux01 = Flux.just("aa", "bb", "cc");

        Flux<String> flux02 = Flux.just("11", "22", "33");

        Flux<Tuple2<String, String>> zipFlux = Flux.zip(flux01, flux02);

        StepVerifier.create(zipFlux)
                    .expectNextMatches(p -> p.getT1().equals("aa") && p.getT2().equals("11"))
                    .expectNextMatches(p -> p.getT1().equals("bb") && p.getT2().equals("22"))
                    .expectNextMatches(p -> p.getT1().equals("cc") && p.getT2().equals("33"))
                    .verifyComplete();
    }

    @Test
    public void zipFluxesToObject() {

        Flux<String> flux01 = Flux.just("aa", "bb", "cc");

        Flux<String> flux02 = Flux.just("11", "22", "33");

        Flux<String> zipFlux = Flux.zip(flux01, flux02, (a, b) -> a + " :: " + b);

        StepVerifier.create(zipFlux).expectNext("aa :: 11").expectNext("bb :: 22")
                    .expectNext("cc :: 33").verifyComplete();
    }

    @Test
    public void firstWithSignalFlux() {

        Flux<String> flux01 = Flux.just("aa", "bb", "cc").delayElements(Duration.ofMillis(100));

        Flux<String> flux02 = Flux.just("11", "22", "33");

        Flux<String> firstFlux = Flux.firstWithSignal(flux01, flux02);

        StepVerifier.create(firstFlux).expectNext("11").expectNext("22").expectNext("33")
                    .verifyComplete();
    }

    @Test
    public void skipFew() {

        Flux<String> flux = Flux.just("11", "22", "33", "44", "55").skip(3);

        StepVerifier.create(flux).expectNext("44").expectNext("55").verifyComplete();
    }

    @Test
    public void skipFewSeconds() {

        Flux<String> flux = Flux.just("11", "22", "33", "44", "55")
                                .delayElements(Duration.ofSeconds(1)).skip(Duration.ofSeconds(4));

        StepVerifier.create(flux).expectNext("44").expectNext("55").verifyComplete();
    }

    @Test
    public void take() {

        Flux<String> flux = Flux.just("11", "22", "33", "44", "55").take(3);

        StepVerifier.create(flux).expectNext("11").expectNext("22").expectNext("33")
                    .verifyComplete();
    }

    @Test
    public void takeForWhile() {

        Flux<String> flux = Flux.just("11", "22", "33", "44", "55")
                                .delayElements(Duration.ofSeconds(1)).take(Duration.ofMillis(3500));

        StepVerifier.create(flux).expectNext("11").expectNext("22").expectNext("33")
                    .verifyComplete();
    }

    @Test
    public void filter() {

        Flux<String> flux = Flux.just("11", "2 2", "33", "4 4", "55")
                                .filter(np -> !np.contains(" "));

        StepVerifier.create(flux).expectNext("11").expectNext("33").expectNext("55")
                    .verifyComplete();
    }

    @Test
    public void distinct() {

        Flux<String> flux = Flux.just("11", "22", "22", "11", "55").distinct();

        StepVerifier.create(flux).expectNext("11", "22", "55").verifyComplete();
    }

    @Test
    public void map() {

        Flux<Fmap> fluxMap = Flux.just("aa 11", "bb 22", "cc 33").map(n -> {
            String[] split = n.split("\\s");
            return new Fmap(split[0], split[1]);
        });

        StepVerifier.create(fluxMap).expectNext(new Fmap("aa", "11"))
                    .expectNext(new Fmap("bb", "22")).expectNext(new Fmap("cc", "33"))
                    .verifyComplete();

    }

    @Test
    public void flatMap() {

        Flux<Fmap> fluxMap = Flux.just("aa 11", "bb 22", "cc 33")
                                 .flatMap(n -> Mono.just(n).map(p -> {
                                     String[] split = p.split("\\s");
                                     return new Fmap(split[0], split[1]);
                                 }).subscribeOn(Schedulers.parallel()));

        List<Fmap> list = Arrays.asList(new Fmap("aa", "11"), new Fmap("bb", "22"),
                                        new Fmap("cc", "33"));

        StepVerifier.create(fluxMap).expectNextMatches(list::contains)
                    .expectNextMatches(list::contains).expectNextMatches(list::contains)
                    .verifyComplete();

    }

    @Test
    public void buffer() {

        Flux<String> flux = Flux.just("11", "22", "33", "44", "55");

        var bufferFlux = flux.buffer(3);

        StepVerifier.create(bufferFlux).expectNext(Arrays.asList("11", "22", "33"))
                    .expectNext(Arrays.asList("44", "55")).verifyComplete();

    }

    @Test
    public void bufferFlatMap() {

        var bufferFlux = Flux.just("a", "b", "c", "d", "e").buffer(3).flatMap(
                f -> Flux.fromIterable(f).map(String::toUpperCase)
                         .subscribeOn(Schedulers.parallel()).log()).subscribe();
    }

    @Test
    public void collectList() {

        var flux = Flux.just("a", "b", "c", "d", "e");

        Mono<List<String>> fluxList = flux.collectList();

        StepVerifier.create(fluxList).expectNext(Arrays.asList("a", "b", "c", "d", "e"))
                    .verifyComplete();
    }

    @Test
    public void collectMap() {

        var flux = Flux.just("a1", "b2", "c3", "a4", "b5");

        Mono<Map<Character, String>> mapMono = flux.collectMap(a -> a.charAt(0));

        StepVerifier.create(mapMono).expectNextMatches(
                m -> m.size() == 3 && m.get('a').equals("a4") && m.get('b').equals("b5") && m.get(
                        'c').equals("c3")).verifyComplete();
    }

    @Test
    public void all() {

        var flux = Flux.just("ba1", "a2", "ba3", "ba4", "ab5");
        Mono<Boolean> fluxHasA = flux.all(a -> a.contains("a"));
        StepVerifier.create(fluxHasA).expectNext(true).verifyComplete();

        Mono<Boolean> fluxHasD = flux.all(a -> a.contains("b"));
        StepVerifier.create(fluxHasD).expectNext(false).verifyComplete();
    }

    @Test
    public void any() {

        var flux = Flux.just("ba1", "a2", "ba3", "ba4", "ab5");
        Mono<Boolean> fluxAnyB = flux.any(a -> a.contains("b"));
        StepVerifier.create(fluxAnyB).expectNext(true).verifyComplete();

        Mono<Boolean> fluxAnyC = flux.any(a -> a.contains("c"));
        StepVerifier.create(fluxAnyC).expectNext(false).verifyComplete();
    }

}


