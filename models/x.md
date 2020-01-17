> All models are wrong. But some are useful. - George Box

Optimizing the website and app (digital assets) is now a core and critical functionality for both product managers and digital marketers. Consider these typical questions faced by these teams: What is the right price for a product to improve conversion? What movies to show for a particular user to improve engagement? The current standard pratice is to do A/B testing.

A/B testing is a method of comparing two or more versions/variations of an entity (eg. webpage color, recommendation etc.) against each other to determine which one performs better for a given conversion goal. But this suffers from two major problems:

a) The test happens for a period of time. Until the test finishes, the results are not communicated to the stakeholders.

b) Since there is no immediate feedback, the experiment remains static for the testing period.

The factors that influence users during the testing period are typically ignored. These sound very similar to the "waterfall" model of software development. The process is very reactive.

Can we make the experimentation process more proactive? Is there a way to make interventions and change the test? Is it possible to continuously get real-time insights?

This is easily achieved using Multi-arm Bandit(MAB) family of algorithms.A multi-armed bandit solution is a ‘smarter’ or more complex version of A/B testing that uses machine learning algorithms to dynamically allocate more traffic to variations that are performing well, while allocating less traffic to variations which are underperforming. Unlike classical A/B testing, multi-armed bandits produces faster results as there is no need to wait for a single winning variation. It uses an explore-exploit framework to continuously test the efficacy of the "winning" approach.

MAB helps users algorithmically capture more value from their experiments by using two strategies, either by reducing the time to statistical significance or by increasing the number of conversions gathered. In more details, 

a) Accelerate Convergence - This algorithm seeks to reduce experiment duration by directing more traffic to the variations that have a better chance of reaching statistical significance, thereby reducing time waiting for results. 

b) Accelerate Reward - This algorithm maximizes the reward/payoff of the experiment by directing more traffic to the leading variation. It helps to exploit the leading variation as much as possible during the experiment lifecycle, thereby avoiding the cost of showing sub-optimal variations.

While these techniques have been known for a long time now, there didn't exist any open source toolkit for developers to help their stakeholders do dynamic A/B testing. Proprietary tools didn't offer a rich choices of the algorithms either.

The speakers, with years of experience building digital businesses (both at startups and at Enterprises) built an open-source toolkit in Python for dynamic and real-time experimentation  - *pybandit*.

The speakers show how they used  pybandit and integrated that with existing open source data engineering and visual analytics packages to help one of their e-commerce customers roll out dynamic pricing of their products in their e-commerce website.

The speakers also discuss the production architecture, that comprised entirely of only open-source tools. 

Outline

1. Introduction to problem: Dynamic pricing in e-commerce
2. Current practice: A/B testing - overview of A/B testing
3. Challenges in current practice
4. Solution: Multi-arm bandit
5. Overview of Multi-arm bandit
6. Case study recap: Dynamic pricing 
7. Data Engineering architecture 
8. A/B testing using pybandit
9. Overview of pybandit and its key features
10. Handling dynamic variations (interventions)
11. Visual real-time analytics for the marketer (the creator of A/B testing)
12. Visual Diagnostics of the model (model interpretability overview and dashboard)
13. Learnings and Pitfalls to watch out for