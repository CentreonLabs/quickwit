"""Design system and static figures for the bench report.

Kept apart from `report.py` so the prose and the data plumbing stay readable.

Palette: a green-shifted neutral ground (the subject is a log stream, and the
neutral is biased toward the NATS series colour rather than left a flat grey),
jade for the NATS series and as the single accent, navy for the Kafka series,
brick reserved for wasted work. Type: Archivo for headings, Literata for prose,
IBM Plex Mono wherever digits line up.
"""

HEAD = """<title>NATS vs Kafka Source Throughput</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Archivo:wght@500;600;700&family=Literata:ital,opsz,wght@0,7..72,400;0,7..72,600;1,7..72,400&family=IBM+Plex+Mono:wght@400;500;600&display=swap">
<style>
:root{
  --ground:#f6f8f5; --surface:#ffffff; --sunk:#eef1ec;
  --ink:#131a17; --ink-soft:#39443e; --muted:#5b6661; --rule:#dfe4dd;
  --jade:#1a6e56; --navy:#2c4e8a; --brick:#a8391f;
  --jade-fill:#d9e9e1; --navy-fill:#dde5f2; --brick-fill:#f4e0d9;
  --shadow:0 1px 2px rgba(19,26,23,.06), 0 8px 24px -16px rgba(19,26,23,.18);
}
@media (prefers-color-scheme: dark){
  :root:not([data-theme="light"]){
    --ground:#0d1110; --surface:#151a18; --sunk:#1b211e;
    --ink:#e7ede9; --ink-soft:#c2ccc6; --muted:#94a09a; --rule:#242c28;
    --jade:#54c39a; --navy:#84a9e4; --brick:#e37a5e;
    --jade-fill:#17322a; --navy-fill:#1a2740; --brick-fill:#361d16;
    --shadow:0 1px 2px rgba(0,0,0,.4), 0 8px 24px -16px rgba(0,0,0,.6);
  }
}
:root[data-theme="dark"]{
  --ground:#0d1110; --surface:#151a18; --sunk:#1b211e;
  --ink:#e7ede9; --ink-soft:#c2ccc6; --muted:#94a09a; --rule:#242c28;
  --jade:#54c39a; --navy:#84a9e4; --brick:#e37a5e;
  --jade-fill:#17322a; --navy-fill:#1a2740; --brick-fill:#361d16;
  --shadow:0 1px 2px rgba(0,0,0,.4), 0 8px 24px -16px rgba(0,0,0,.6);
}

*{box-sizing:border-box}
body{
  margin:0; background:var(--ground); color:var(--ink);
  font-family:Literata,Georgia,"Times New Roman",serif;
  font-size:17px; line-height:1.62;
  -webkit-font-smoothing:antialiased;
}
.page{max-width:1080px; margin:0 auto; padding:clamp(2rem,5vw,4.5rem) clamp(1.1rem,4vw,2.5rem) 6rem}
.col{max-width:66ch}

h1,h2,h3,.eyebrow,th,.tile-value,.num{font-family:Archivo,"Helvetica Neue",Arial,sans-serif}
h1{
  font-size:clamp(2.1rem,5.2vw,3.4rem); line-height:1.04; font-weight:700;
  letter-spacing:-.028em; margin:0 0 1rem; text-wrap:balance;
}
h2{
  font-size:clamp(1.35rem,2.8vw,1.85rem); line-height:1.16; font-weight:650;
  letter-spacing:-.02em; margin:0 0 .5rem; text-wrap:balance;
}
h3{
  font-size:1.02rem; font-weight:600; letter-spacing:-.008em;
  margin:2.2rem 0 .4rem; text-wrap:balance;
}
p{margin:0 0 1.05rem}
a{color:var(--jade); text-underline-offset:3px; text-decoration-thickness:1px}
strong{font-weight:600}

.eyebrow{
  font-family:"IBM Plex Mono",ui-monospace,monospace;
  font-size:.7rem; font-weight:500; letter-spacing:.16em; text-transform:uppercase;
  color:var(--muted); margin:0 0 .55rem;
}
.lede{font-size:1.16rem; line-height:1.55; color:var(--ink-soft); margin-bottom:1.6rem}
.meta{
  font-family:"IBM Plex Mono",ui-monospace,monospace; font-size:.78rem;
  color:var(--muted); line-height:1.75; margin:0;
}
code,.mono{font-family:"IBM Plex Mono",ui-monospace,monospace; font-size:.88em}
code{background:var(--sunk); padding:.1em .34em; border-radius:3px}

section{margin-top:clamp(3rem,6vw,4.6rem)}
hr{border:0; border-top:1px solid var(--rule); margin:0}

/* ---- stat tiles ---- */
.tiles{display:grid; grid-template-columns:repeat(auto-fit,minmax(215px,1fr)); gap:1px;
  background:var(--rule); border:1px solid var(--rule); border-radius:6px; overflow:hidden; margin:2rem 0}
.tile{background:var(--surface); padding:1.2rem 1.3rem 1.35rem}
.tile-label{font-family:"IBM Plex Mono",ui-monospace,monospace; font-size:.68rem;
  letter-spacing:.13em; text-transform:uppercase; color:var(--muted); margin:0 0 .5rem}
.tile-value{font-size:1.95rem; font-weight:700; letter-spacing:-.03em; line-height:1;
  font-variant-numeric:tabular-nums; margin:0 0 .4rem}
.tile-note{font-size:.86rem; line-height:1.45; color:var(--muted); margin:0}
.v-jade{color:var(--jade)} .v-navy{color:var(--navy)} .v-brick{color:var(--brick)}

/* ---- tables ---- */
.scroll{overflow-x:auto; border:1px solid var(--rule); border-radius:6px;
  background:var(--surface); box-shadow:var(--shadow); margin:1.6rem 0}
table{border-collapse:collapse; width:100%; min-width:820px;
  font-family:"IBM Plex Mono",ui-monospace,monospace; font-size:.8rem}
caption{text-align:left; padding:.9rem 1.1rem .2rem; font-family:Literata,serif;
  font-size:.9rem; color:var(--muted); caption-side:top}
th{
  text-align:right; font-size:.66rem; font-weight:600; letter-spacing:.09em;
  text-transform:uppercase; color:var(--muted); padding:.85rem .7rem .55rem;
  border-bottom:1px solid var(--rule); white-space:nowrap; position:sticky; top:0;
  background:var(--surface);
}
th.l,td.l{text-align:left}
td{padding:.5rem .7rem; text-align:right; font-variant-numeric:tabular-nums;
  border-bottom:1px solid var(--rule); white-space:nowrap}
tbody tr:last-child td{border-bottom:0}
tbody tr.group-end td{border-bottom:2px solid var(--rule)}
td.name{font-weight:500; color:var(--ink)}
.src-nats{color:var(--jade); font-weight:600}
.src-kafka{color:var(--navy); font-weight:600}
.bad{color:var(--brick); font-weight:600}
.dim{color:var(--muted)}

.pill{display:inline-block; padding:.08rem .42rem; border-radius:3px; font-size:.68rem;
  font-weight:600; letter-spacing:.04em}
.pill-on{background:var(--brick-fill); color:var(--brick)}
.pill-off{background:var(--sunk); color:var(--muted)}

/* ---- bar chart ---- */
.bars{display:flex; flex-direction:column; gap:.5rem; margin:1.7rem 0}
.bar-row{display:grid; grid-template-columns:minmax(120px,190px) 1fr auto; gap:.85rem;
  align-items:center; font-family:"IBM Plex Mono",ui-monospace,monospace; font-size:.76rem}
.bar-label{color:var(--ink-soft); text-align:right; white-space:nowrap; overflow:hidden;
  text-overflow:ellipsis}
.bar-track{background:var(--sunk); border-radius:2px; height:22px; position:relative; overflow:hidden}
.bar-fill{height:100%; border-radius:2px 0 0 2px}
.bar-waste{height:100%; position:absolute; top:0;
  background:repeating-linear-gradient(135deg,var(--brick-fill) 0 4px,transparent 4px 8px);
  border-left:1px solid var(--brick)}
.bar-value{font-variant-numeric:tabular-nums; color:var(--ink); min-width:5.6rem; text-align:right}
.legend{display:flex; flex-wrap:wrap; gap:1.1rem; font-family:"IBM Plex Mono",ui-monospace,monospace;
  font-size:.72rem; color:var(--muted); margin:.3rem 0 0}
.legend span{display:flex; align-items:center; gap:.42rem}
.swatch{width:11px; height:11px; border-radius:2px; flex:none}
.swatch-waste{background:repeating-linear-gradient(135deg,var(--brick-fill) 0 4px,transparent 4px 8px);
  border:1px solid var(--brick)}

/* ---- figures ---- */
figure{margin:2rem 0; padding:0}
figure svg{max-width:100%; height:auto; display:block; margin:0 auto}
figcaption{font-size:.9rem; line-height:1.5; color:var(--muted); margin-top:.85rem; max-width:62ch}
.fig-shell{background:var(--surface); border:1px solid var(--rule); border-radius:6px;
  padding:1.5rem 1.3rem 1.2rem; box-shadow:var(--shadow); overflow-x:auto}

/* ---- callout ---- */
.callout{background:var(--surface); border:1px solid var(--rule); border-left:3px solid var(--jade);
  border-radius:0 6px 6px 0; padding:1.05rem 1.25rem; margin:1.6rem 0}
.callout.warn{border-left-color:var(--brick)}
.callout p:last-child{margin-bottom:0}
.callout .eyebrow{margin-bottom:.35rem}

ul,ol{margin:0 0 1.05rem; padding-left:1.35rem}
li{margin-bottom:.5rem}
li::marker{color:var(--muted)}

:focus-visible{outline:2px solid var(--jade); outline-offset:2px}
@media (prefers-reduced-motion:no-preference){
  .bar-fill,.bar-waste{transition:width .4s ease}
}
</style>
"""


# One pipeline's timeline in both modes. The claim the figure has to carry is
# that the sleep is not a one-off startup cost at 16 pipelines: every burst ends
# when the indexer's mailbox drains, and the rest of the cycle is dead time.
FIG_COOP = """
<figure>
<div class="fig-shell">
<svg viewBox="0 0 780 262" role="img"
     aria-label="Timeline of one indexing pipeline. With cooperative indexing off it works continuously. With it on, the pipeline sleeps until its assigned phase, then works only until its mailbox drains, then sleeps out the rest of each 60-second commit-timeout cycle.">
  <defs>
    <marker id="ar" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0,1 L9,5 L0,9 z" fill="currentColor"/>
    </marker>
  </defs>

  <!-- cycle boundaries -->
  <g stroke="currentColor" stroke-width="1" stroke-dasharray="3 4" opacity=".28">
    <line x1="350" y1="30" x2="350" y2="206"/>
    <line x1="550" y1="30" x2="550" y2="206"/>
    <line x1="750" y1="30" x2="750" y2="206"/>
  </g>

  <!-- commit timeout bracket -->
  <g stroke="currentColor" fill="none" opacity=".55">
    <path d="M150,26 L150,20 L350,20 L350,26" stroke-width="1"/>
  </g>
  <text x="250" y="14" text-anchor="middle" font-size="11" fill="currentColor" opacity=".75"
        font-family="IBM Plex Mono, monospace">commit_timeout = 60 s</text>

  <!-- row A: coop off -->
  <text x="140" y="60" text-anchor="end" font-size="12" fill="currentColor"
        font-family="IBM Plex Mono, monospace">coop off</text>
  <rect x="150" y="46" width="196" height="24" rx="2" fill="var(--navy)"/>
  <text x="248" y="62" text-anchor="middle" font-size="11" fill="var(--surface)"
        font-family="IBM Plex Mono, monospace">indexing</text>
  <text x="356" y="62" font-size="11" fill="currentColor" opacity=".7"
        font-family="IBM Plex Mono, monospace">corpus drained</text>

  <!-- row B: coop on -->
  <text x="140" y="130" text-anchor="end" font-size="12" fill="currentColor"
        font-family="IBM Plex Mono, monospace">coop on</text>
  <rect x="150" y="116" width="130" height="24" rx="2" fill="none"
        stroke="currentColor" stroke-width="1" stroke-dasharray="4 3" opacity=".5"/>
  <text x="215" y="132" text-anchor="middle" font-size="11" fill="currentColor" opacity=".7"
        font-family="IBM Plex Mono, monospace">sleep</text>
  <rect x="280" y="116" width="34" height="24" rx="2" fill="var(--jade)"/>
  <rect x="314" y="116" width="166" height="24" rx="2" fill="none"
        stroke="currentColor" stroke-width="1" stroke-dasharray="4 3" opacity=".5"/>
  <text x="397" y="132" text-anchor="middle" font-size="11" fill="currentColor" opacity=".7"
        font-family="IBM Plex Mono, monospace">sleep</text>
  <rect x="480" y="116" width="34" height="24" rx="2" fill="var(--jade)"/>
  <rect x="514" y="116" width="166" height="24" rx="2" fill="none"
        stroke="currentColor" stroke-width="1" stroke-dasharray="4 3" opacity=".5"/>
  <text x="597" y="132" text-anchor="middle" font-size="11" fill="currentColor" opacity=".7"
        font-family="IBM Plex Mono, monospace">sleep</text>
  <rect x="680" y="116" width="34" height="24" rx="2" fill="var(--jade)"/>

  <!-- annotations -->
  <g stroke="var(--jade)" fill="none" stroke-width="1.2">
    <path d="M297,150 L297,172" marker-end="url(#ar)"/>
  </g>
  <text x="303" y="178" font-size="11" fill="var(--jade)" font-family="IBM Plex Mono, monospace">
    mailbox drains &#8594; split cut, permit released, sleep(60 s &#8722; work)
  </text>
  <g stroke="currentColor" fill="none" stroke-width="1.2" opacity=".6">
    <path d="M215,108 L215,92" marker-end="url(#ar)"/>
  </g>
  <text x="215" y="86" text-anchor="middle" font-size="11" fill="currentColor" opacity=".7"
        font-family="IBM Plex Mono, monospace">initial sleep = hash(pipeline) mod commit_timeout</text>

  <!-- axis -->
  <line x1="150" y1="206" x2="750" y2="206" stroke="currentColor" stroke-width="1" opacity=".45"/>
  <g font-size="11" fill="currentColor" opacity=".6" font-family="IBM Plex Mono, monospace"
     text-anchor="middle">
    <text x="150" y="224">0 s</text>
    <text x="350" y="224">60</text>
    <text x="550" y="224">120</text>
    <text x="750" y="224">180</text>
  </g>
  <text x="450" y="248" text-anchor="middle" font-size="11" fill="currentColor" opacity=".55"
        font-family="IBM Plex Mono, monospace">wall clock</text>
</svg>
</div>
<figcaption>Cooperative indexing does not slow the indexer down while it runs &mdash; it
decides <em>when</em> it runs. The indexer takes one of <code>num_blocking_threads</code>
permits per split, and once its mailbox drains it cuts the split and sleeps out the rest of a
<code>commit_timeout</code>-long cycle. One source cannot keep one indexer's mailbox full, so
with a backlog waiting every cycle looks like the bottom row.</figcaption>
</figure>
"""


# The comparison the report turns on: both 16-way splits cost something, but not
# the same thing. Kafka's pipelines share a consumer group and one checkpoint, so
# a rebalance makes two of them publish the same offset range and one loses.
# NATS gives each source a disjoint filter and its own checkpoint -- nothing to
# lose -- but every filtered consumer still walks the whole stream.
FIG_SPLIT_TEMPLATE = """
<figure>
<div class="fig-shell">
<svg viewBox="0 0 1000 336" role="img"
     aria-label="Left: Kafka's sixteen pipelines share one consumer group and one source checkpoint, so a rebalance makes two pipelines publish the same offset range and the metastore rejects the second, faulting the pipeline into re-reading. Right: each NATS source has a disjoint subject filter and its own checkpoint, so nothing is duplicated, but every filtered consumer walks all sixteen subjects to keep one.">
  <defs>
    <marker id="ar2" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0,1 L9,5 L0,9 z" fill="currentColor"/>
    </marker>
  </defs>

  <line x1="470" y1="16" x2="470" y2="320" stroke="currentColor" stroke-width="1" opacity=".18"/>

  <!-- ================= KAFKA ================= -->
  <text x="40" y="24" font-size="11" font-weight="600" fill="var(--navy)"
        font-family="IBM Plex Mono, monospace" letter-spacing="1">KAFKA &#183; 1 SOURCE, 16 PIPELINES</text>

  <rect x="40" y="40" width="390" height="30" rx="3" fill="var(--navy-fill)"
        stroke="var(--navy)" stroke-width="1"/>
  <text x="235" y="60" text-anchor="middle" font-size="11.5" fill="currentColor"
        font-family="IBM Plex Mono, monospace">topic logs &#183; 16 partitions</text>

  <rect x="40" y="118" width="390" height="66" rx="4" fill="none" stroke="currentColor"
        stroke-width="1" stroke-dasharray="4 3" opacity=".45"/>
  <text x="46" y="112" font-size="10.5" fill="currentColor" opacity=".65"
        font-family="IBM Plex Mono, monospace">one consumer group</text>
  <rect x="62" y="132" width="118" height="38" rx="3" fill="var(--surface)"
        stroke="var(--navy)" stroke-width="1"/>
  <text x="121" y="156" text-anchor="middle" font-size="11" fill="currentColor"
        font-family="IBM Plex Mono, monospace">pipeline A</text>
  <rect x="290" y="132" width="118" height="38" rx="3" fill="var(--surface)"
        stroke="var(--navy)" stroke-width="1"/>
  <text x="349" y="156" text-anchor="middle" font-size="11" fill="currentColor"
        font-family="IBM Plex Mono, monospace">pipeline B</text>

  <g stroke="var(--brick)" fill="none" stroke-width="1.3" color="var(--brick)">
    <path d="M180,144 C214,116 256,116 290,144" marker-end="url(#ar2)"/>
  </g>
  <text x="235" y="104" text-anchor="middle" font-size="10.5" fill="var(--brick)"
        font-family="IBM Plex Mono, monospace">rebalance hands partition 12 over</text>

  <rect x="140" y="222" width="180" height="34" rx="3" fill="var(--sunk)"
        stroke="currentColor" stroke-width="1" opacity=".9"/>
  <text x="230" y="243" text-anchor="middle" font-size="11" fill="currentColor"
        font-family="IBM Plex Mono, monospace">one source checkpoint</text>

  <g stroke-width="1.3" fill="none">
    <path d="M121,170 L121,206 L186,206 L186,222" stroke="currentColor" color="currentColor"
          marker-end="url(#ar2)" opacity=".7"/>
    <path d="M349,170 L349,206 L276,206 L276,222" stroke="var(--brick)" color="var(--brick)"
          marker-end="url(#ar2)"/>
  </g>
  <text x="112" y="200" text-anchor="end" font-size="10.5" fill="currentColor" opacity=".7"
        font-family="IBM Plex Mono, monospace">&#916; kept</text>
  <text x="360" y="200" font-size="10.5" fill="var(--brick)"
        font-family="IBM Plex Mono, monospace">&#916; rejected</text>

  <text x="40" y="284" font-size="11" fill="var(--brick)" font-family="IBM Plex Mono, monospace">
    incompatible checkpoint delta &#8594; publisher faults
  </text>
  <text x="40" y="302" font-size="11" fill="var(--brick)" font-family="IBM Plex Mono, monospace">
    &#8594; pipeline re-reads. Measured dup {kafka_dup}&#215;.
  </text>

  <!-- ================= NATS ================= -->
  <text x="500" y="24" font-size="11" font-weight="600" fill="var(--jade)"
        font-family="IBM Plex Mono, monospace" letter-spacing="1">NATS &#183; 16 SOURCES, 1 PIPELINE EACH</text>

  <rect x="500" y="40" width="380" height="30" rx="3" fill="var(--jade-fill)"
        stroke="var(--jade)" stroke-width="1"/>
  <text x="690" y="60" text-anchor="middle" font-size="11.5" fill="currentColor"
        font-family="IBM Plex Mono, monospace">stream logs &#183; 16 subjects</text>

  <!-- three of the sixteen consumers; each walks all 16 subjects, keeps one -->
  <g stroke="var(--jade)" stroke-width="1" fill="none" opacity=".45">
    <rect x="502" y="100" width="14" height="14" rx="1"/><rect x="526" y="100" width="14" height="14" rx="1"/>
    <rect x="550" y="100" width="14" height="14" rx="1"/><rect x="574" y="100" width="14" height="14" rx="1"/>
    <rect x="598" y="100" width="14" height="14" rx="1"/><rect x="622" y="100" width="14" height="14" rx="1"/>
    <rect x="646" y="100" width="14" height="14" rx="1"/><rect x="670" y="100" width="14" height="14" rx="1"/>
    <rect x="694" y="100" width="14" height="14" rx="1"/><rect x="718" y="100" width="14" height="14" rx="1"/>
    <rect x="742" y="100" width="14" height="14" rx="1"/><rect x="766" y="100" width="14" height="14" rx="1"/>
    <rect x="790" y="100" width="14" height="14" rx="1"/><rect x="814" y="100" width="14" height="14" rx="1"/>
    <rect x="838" y="100" width="14" height="14" rx="1"/><rect x="862" y="100" width="14" height="14" rx="1"/>
    <rect x="502" y="130" width="14" height="14" rx="1"/><rect x="526" y="130" width="14" height="14" rx="1"/>
    <rect x="550" y="130" width="14" height="14" rx="1"/><rect x="574" y="130" width="14" height="14" rx="1"/>
    <rect x="598" y="130" width="14" height="14" rx="1"/><rect x="622" y="130" width="14" height="14" rx="1"/>
    <rect x="646" y="130" width="14" height="14" rx="1"/><rect x="670" y="130" width="14" height="14" rx="1"/>
    <rect x="694" y="130" width="14" height="14" rx="1"/><rect x="718" y="130" width="14" height="14" rx="1"/>
    <rect x="742" y="130" width="14" height="14" rx="1"/><rect x="766" y="130" width="14" height="14" rx="1"/>
    <rect x="790" y="130" width="14" height="14" rx="1"/><rect x="814" y="130" width="14" height="14" rx="1"/>
    <rect x="838" y="130" width="14" height="14" rx="1"/><rect x="862" y="130" width="14" height="14" rx="1"/>
    <rect x="502" y="184" width="14" height="14" rx="1"/><rect x="526" y="184" width="14" height="14" rx="1"/>
    <rect x="550" y="184" width="14" height="14" rx="1"/><rect x="574" y="184" width="14" height="14" rx="1"/>
    <rect x="598" y="184" width="14" height="14" rx="1"/><rect x="622" y="184" width="14" height="14" rx="1"/>
    <rect x="646" y="184" width="14" height="14" rx="1"/><rect x="670" y="184" width="14" height="14" rx="1"/>
    <rect x="694" y="184" width="14" height="14" rx="1"/><rect x="718" y="184" width="14" height="14" rx="1"/>
    <rect x="742" y="184" width="14" height="14" rx="1"/><rect x="766" y="184" width="14" height="14" rx="1"/>
    <rect x="790" y="184" width="14" height="14" rx="1"/><rect x="814" y="184" width="14" height="14" rx="1"/>
    <rect x="838" y="184" width="14" height="14" rx="1"/><rect x="862" y="184" width="14" height="14" rx="1"/>
  </g>
  <g fill="var(--jade)">
    <rect x="502" y="100" width="14" height="14" rx="1"/>
    <rect x="526" y="130" width="14" height="14" rx="1"/>
    <rect x="862" y="184" width="14" height="14" rx="1"/>
  </g>
  <text x="690" y="168" text-anchor="middle" font-size="13" fill="currentColor" opacity=".45"
        font-family="IBM Plex Mono, monospace">&#8942;</text>

  <text x="500" y="230" font-size="11" fill="var(--jade)" font-family="IBM Plex Mono, monospace">
    filled = the one subject a source keeps
  </text>
  <text x="500" y="248" font-size="11" fill="currentColor" opacity=".72"
        font-family="IBM Plex Mono, monospace">
    disjoint filters, one checkpoint each &#8594; dup 1.00
  </text>
  <text x="500" y="284" font-size="11" fill="var(--brick)" font-family="IBM Plex Mono, monospace">
    but each consumer walks all 16 to keep 1, so the
  </text>
  <text x="500" y="302" font-size="11" fill="var(--brick)" font-family="IBM Plex Mono, monospace">
    broker replays the stream 16&#215; on catch-up.
  </text>
</svg>
</div>
<figcaption>Splitting the backlog 16 ways costs something on both sides, but not the
same thing. Kafka loses work to rebalances; NATS loses nothing but makes the broker replay the
stream once per source, which is what saturates <code>nats-server</code>.</figcaption>
</figure>
"""
