(WITH ResponsePairs AS (
    SELECT DISTINCT 
        a.name AS TaskA, 
        b.name AS TaskB, 
        a.trace_id
    FROM event a
    JOIN event b 
        ON a.trace_id = b.trace_id 
        AND a.name <> b.name 
        AND a.time < b.time
),
TraceCounts AS (
    SELECT 
        TaskA, 
        COUNT(DISTINCT trace_id) AS TotalTraces
    FROM ResponsePairs
    GROUP BY TaskA
),
PairCoverage AS (
    SELECT 
        r1.TaskA, 
        ARRAY[r1.TaskB, r2.TaskB] AS TargetSet, 
        COUNT(DISTINCT r1.trace_id) AS JointCoverage
    FROM ResponsePairs r1
    JOIN ResponsePairs r2 
        ON r1.TaskA = r2.TaskA 
        AND r1.TaskB < r2.TaskB
        AND r1.trace_id = r2.trace_id
    GROUP BY r1.TaskA, r1.TaskB, r2.TaskB
),
OptimalTargets AS (
    SELECT DISTINCT ON (p.TaskA) 
        p.TaskA, 
        p.TargetSet, 
        p.JointCoverage,
        CAST(p.JointCoverage AS FLOAT) / CAST(tc.TotalTraces AS FLOAT) AS Support
    FROM PairCoverage p
    JOIN TraceCounts tc ON p.TaskA = tc.TaskA
    ORDER BY p.TaskA, Support DESC
)
SELECT 
    'Response' AS ConstraintType,
    o.TaskA, 
    o.TargetSet, 
    o.Support
FROM OptimalTargets o
WHERE o.Support >= 0.5)
UNION ALL
(WITH AlternatePairs AS (
    SELECT DISTINCT 
        a.name AS TaskA, 
        b.name AS TaskB, 
        a.trace_id
    FROM event a
    JOIN event b 
        ON a.trace_id = b.trace_id 
        AND a.name <> b.name 
        AND b.time > a.time 
        AND NOT EXISTS (
            SELECT 1 FROM event c
            WHERE c.trace_id = a.trace_id
            AND c.time > a.time 
            AND c.time < b.time
            AND c.name = a.name
        )
),
TraceCounts AS (
    SELECT 
        TaskA, 
        COUNT(DISTINCT trace_id) AS TotalTraces
    FROM AlternatePairs
    GROUP BY TaskA
),
PairCoverage AS (
    SELECT 
        r1.TaskA, 
        ARRAY[r1.TaskB, r2.TaskB] AS TargetSet, 
        COUNT(DISTINCT r1.trace_id) AS JointCoverage
    FROM AlternatePairs r1
    JOIN AlternatePairs r2 
        ON r1.TaskA = r2.TaskA 
        AND r1.TaskB < r2.TaskB
        AND r1.trace_id = r2.trace_id
    GROUP BY r1.TaskA, r1.TaskB, r2.TaskB
),
OptimalTargets AS (
    SELECT DISTINCT ON (p.TaskA) 
        p.TaskA, 
        p.TargetSet, 
        p.JointCoverage,
        CAST(p.JointCoverage AS FLOAT) / CAST(tc.TotalTraces AS FLOAT) AS Support
    FROM PairCoverage p
    JOIN TraceCounts tc ON p.TaskA = tc.TaskA
    ORDER BY p.TaskA, Support DESC
)
SELECT 
    'AlternateResponse' AS ConstraintType,
    o.TaskA, 
    o.TargetSet, 
    o.Support
FROM OptimalTargets o
WHERE o.Support >= 0.5)

UNION ALL
(WITH ChainResponsePairs AS (
    SELECT DISTINCT
        a.name AS TaskA,
        b.name AS TaskB,
        a.trace_id
    FROM event a
    JOIN event b
        ON a.trace_id = b.trace_id
        AND a.name <> b.name
        AND b.time > a.time
    AND NOT EXISTS (
        SELECT 1
        FROM event c
        WHERE c.trace_id = a.trace_id
        AND c.time > a.time
        AND c.time < b.time
        AND c.name <> b.name
    )
),
TraceCounts AS (
    SELECT 
        TaskA, 
        COUNT(DISTINCT trace_id) AS TotalTraces
    FROM ChainResponsePairs
    GROUP BY TaskA
),
PairCoverage AS (
    SELECT 
        c1.TaskA, 
        ARRAY[c1.TaskB, c2.TaskB] AS TargetSet, 
        COUNT(DISTINCT c1.trace_id) AS JointCoverage
    FROM ChainResponsePairs c1
    JOIN ChainResponsePairs c2 
        ON c1.TaskA = c2.TaskA 
        AND c1.TaskB < c2.TaskB
        AND c1.trace_id = c2.trace_id
    GROUP BY c1.TaskA, c1.TaskB, c2.TaskB
),
OptimalTargets AS (
    SELECT DISTINCT ON (p.TaskA) 
        p.TaskA, 
        p.TargetSet, 
        p.JointCoverage,
        CAST(p.JointCoverage AS FLOAT) / CAST(tc.TotalTraces AS FLOAT) AS Support
    FROM PairCoverage p
    JOIN TraceCounts tc ON p.TaskA = tc.TaskA
    ORDER BY p.TaskA, Support DESC
)
SELECT 
    'ChainResponse' AS ConstraintType,
    o.TaskA, 
    o.TargetSet, 
    o.Support
FROM OptimalTargets o
WHERE o.Support >= 0.5)

UNION ALL

(WITH PrecedencePairs AS (
    SELECT DISTINCT
        a.name AS TaskA,
        b.name AS TaskB,
        a.trace_id
    FROM event a
    JOIN event b
        ON a.trace_id = b.trace_id
        AND a.name <> b.name
        AND b.time > a.time
),
TraceCounts AS (
    SELECT 
        TaskA, 
        COUNT(DISTINCT trace_id) AS TotalTraces
    FROM PrecedencePairs
    GROUP BY TaskA
),
PairCoverage AS (
    SELECT 
        c1.TaskA, 
        ARRAY[c1.TaskB, c2.TaskB] AS TargetSet, 
        COUNT(DISTINCT c1.trace_id) AS JointCoverage
    FROM PrecedencePairs c1
    JOIN PrecedencePairs c2 
        ON c1.TaskA = c2.TaskA
        AND c1.TaskB < c2.TaskB
        AND c1.trace_id = c2.trace_id
    GROUP BY c1.TaskA, c1.TaskB, c2.TaskB
),
OptimalTargets AS (
    SELECT DISTINCT ON (p.TaskA) 
        p.TaskA, 
        p.TargetSet, 
        p.JointCoverage,
        CAST(p.JointCoverage AS FLOAT) / CAST(tc.TotalTraces AS FLOAT) AS Support
    FROM PairCoverage p
    JOIN TraceCounts tc ON p.TaskA = tc.TaskA
    ORDER BY p.TaskA, Support DESC
)
SELECT 
    'Precedence' AS ConstraintType,
    o.TaskA, 
    o.TargetSet, 
    o.Support
FROM OptimalTargets o
WHERE o.Support >= 0.5)

UNION ALL(

----------------------------

WITH AlternatePrecedencePairs AS (
    SELECT DISTINCT
        a.name AS TaskA,
        b.name AS TaskB,
        a.trace_id
    FROM event a
    JOIN event b
        ON a.trace_id = b.trace_id
        AND a.name <> b.name
        AND a.time < b.time
    	AND NOT EXISTS (
        SELECT 1
        FROM event c
        WHERE c.trace_id = a.trace_id
          AND c.name = b.name
          AND c.time > a.time
          AND c.time < b.time
    )
),
TraceCounts AS (
    SELECT 
        TaskA, 
        COUNT(DISTINCT trace_id) AS TotalTraces
    FROM AlternatePrecedencePairs
    GROUP BY TaskA
),
TargetCoverage AS (
    SELECT 
        c1.TaskA, 
        c1.TaskB AS TaskB1, 
        c2.TaskB AS TaskB2, 
        COUNT(DISTINCT c1.trace_id) AS CoveredTraces
    FROM AlternatePrecedencePairs c1
    JOIN AlternatePrecedencePairs c2 
        ON c1.TaskA = c2.TaskA  -- Same TaskA
        AND c1.TaskB < c2.TaskB  -- Ensure we don't duplicate pairs (TaskB1, TaskB2) and (TaskB2, TaskB1)
        AND c1.trace_id = c2.trace_id  -- Ensure they co-occur in the same trace
    GROUP BY c1.TaskA, c1.TaskB, c2.TaskB
),
OptimalTargets AS (
    SELECT DISTINCT ON (c.TaskA)
        c.TaskA, 
        ARRAY[c.TaskB1, c.TaskB2] AS TargetSet,
        c.CoveredTraces
    FROM TargetCoverage c
    ORDER BY c.TaskA, c.CoveredTraces DESC  -- Keep only the best pair per TaskA
)
SELECT 
    'AlternatePrecedence' AS ConstraintType,
    o.TaskA, 
    o.TargetSet,  -- Keep only a target set of size 2
    CAST(o.CoveredTraces AS FLOAT) / CAST(t.TotalTraces AS FLOAT) AS Support
FROM OptimalTargets o
JOIN TraceCounts t ON o.TaskA = t.TaskA
WHERE o.CoveredTraces > 0  -- Only include pairs with coverage
ORDER BY o.TaskA, o.CoveredTraces DESC)

UNION ALL(
----------------------------
----------------------------
WITH ChainPrecedencePairs AS (
    SELECT DISTINCT
        a.name AS TaskA,
        b.name AS TaskB,
        a.trace_id
    FROM event a
    JOIN event b
        ON a.trace_id = b.trace_id
        AND a.name <> b.name
        AND b.time > a.time
    WHERE NOT EXISTS (
        -- Ensure that there is no other event between a and b in the same trace
        SELECT 1
        FROM event c
        WHERE c.trace_id = a.trace_id
          AND c.time > a.time
          AND c.time < b.time
          AND c.name <> b.name  -- Ensure no other events of type 'b' occur between 'a' and 'b'
    )
),
TraceCounts AS (
    SELECT 
        TaskA, 
        COUNT(DISTINCT trace_id) AS TotalTraces
    FROM ChainPrecedencePairs
    GROUP BY TaskA
),
TargetPairs AS (
    SELECT 
        p1.TaskA, 
        ARRAY[p1.TaskB, p2.TaskB] AS TargetSet, 
        p1.trace_id
    FROM ChainPrecedencePairs p1
    JOIN ChainPrecedencePairs p2
        ON p1.trace_id = p2.trace_id 
        AND p1.TaskA = p2.TaskA 
        AND p1.TaskB < p2.TaskB -- Avoid duplicates and (b1, b1) pairs
),
TargetCoverage AS (
    SELECT 
        TaskA, 
        TargetSet, 
        COUNT(DISTINCT trace_id) AS CoveredTraces
    FROM TargetPairs
    GROUP BY TaskA, TargetSet
),
MaxCoverage AS (
    SELECT 
        TaskA, 
        MAX(CoveredTraces) AS MaxCoveredTraces
    FROM TargetCoverage
    GROUP BY TaskA
)
SELECT 
    'ChainPrecedence' AS ConstraintType,
    t.TaskA, 
    t.TargetSet, 
    CAST(t.CoveredTraces AS FLOAT) / CAST(tc.TotalTraces AS FLOAT) AS Support
FROM TargetCoverage t
JOIN TraceCounts tc ON t.TaskA = tc.TaskA
JOIN MaxCoverage m ON t.TaskA = m.TaskA
WHERE t.CoveredTraces = m.MaxCoveredTraces
  AND CAST(t.CoveredTraces AS FLOAT) / CAST(tc.TotalTraces AS FLOAT) >= 0.5 -- Filter by support >= 0.5
ORDER BY t.TaskA, Support DESC)

