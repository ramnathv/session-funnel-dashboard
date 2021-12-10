import pandas as pd
import plotly.graph_objects as go
import plotly.figure_factory as ff
import plotly.express as px

def aggregate_data(data):
    return (
        data
          .sort_values(by='timestamp')
          .groupby('user_id')
          .cumcount()
          .to_frame(name='step')
          .merge(data, left_index=True, right_index=True)
          .groupby(['step', 'event'])
          ['user_id']
          .sum()
          .reset_index(name='nb_sessions')
          .assign(step_event = lambda d: [f"{x[0]}: {x[1]}" for x in zip(d.step, d.event)])
    )

def enrich_data(data):
    return (
        data
          .sort_values(by='timestamp')
          .groupby('session_id')
          .cumcount()
          .to_frame(name='step')
          .merge(data, left_index=True, right_index=True)
          .assign(step_event = lambda d: [f"{x[0]}: {x[1]}" for x in zip(d.step, d.event)])
          [['session_id', 'event', 'timestamp', 'step', 'step_event']]

    )

def sankify(data_enriched, use_step=True):
    event_col = 'step_event' if use_step else 'event'
    d = (
      data_enriched
        .query('step <= 5')
        .groupby('session_id')
        [[event_col]]
        .shift()
        .rename(columns={event_col: 'event_from'})
        .merge(data_enriched, left_index=True, right_index=True)
        .rename(columns={event_col: 'event_to'})
        .dropna()
        .groupby(['event_from', 'event_to'], as_index=False)
        [['session_id']]
        .count()
        .rename(columns={'session_id': 'nb_sessions'})
    )
    events = list(set(d.event_to.unique().tolist() + d.event_from.unique().tolist()))
    events_map = {event: i for i, event in enumerate(set(events))}
    d2 = (
      d
        .assign(event_from = lambda d: d.event_from.map(events_map))
        .assign(event_to = lambda d: d.event_to.map(events_map))
    )
    if use_step:
        ev = [x.split(": ")[1] for x in events]
    else:
        ev = events

    palette = px.colors.qualitative.Vivid + ['blue', 'green']
    color_map = {l: palette[i] for i, l in enumerate(list(set(ev)))}
    colors = [color_map[ev] for ev in ev]
    return dict(data = d2, events = events, colors = colors)


def visualize_funnel_sankey(data_enriched, use_step=False):
    d = sankify(data_enriched, use_step=use_step)
    fig = go.Figure(data=[go.Sankey(
        node = dict(
          pad = 15,
          thickness = 20,
          line = dict(color = "black", width = 0.5),
          label = d['events'],
          color = d['colors']
        ),
        link = dict(
          source = d['data'].event_from.values,
          target = d['data'].event_to.values,
          value = d['data'].nb_sessions.values,
      ))])

    fig.update_layout(
        title_text="User Journey", 
        font_size=10,
        height=600
    )
    return fig

def visualize_step_matrix(data_enriched):
    df2 = (
        data_enriched
          .drop(columns=['session_id'])
          .sort_values(by='timestamp')
          .query('step <= 14')
          .groupby(['event', 'step'], as_index=False)
          .count()
          .rename(columns={'timestamp': 'nb_sessions'})
          .pivot(index='event', columns='step', values='nb_sessions')
          .fillna(0)
          .apply(lambda d: d / d.sum())
          .sort_values(by=[1, 2, 3], ascending=True)
          .reset_index()
    )
    y = df2.event.tolist()
    x = (df2.columns[1:] + 1).tolist()
    z = df2.drop(columns='event')
    text = z.applymap(lambda x: '{:.1%}'.format(x) if x > 0 else '').values.tolist()

    fig = ff.create_annotated_heatmap(
        x=x, 
        y=[y.capitalize() for y in y], 
        z=z.values, 
        annotation_text=text, 
        colorscale='magma_r'
    )
    fig.update_layout(height=600, title='Step Matrix')
    return fig

def plot_funnel(data, targets, group=None):
    event_group = [t if isinstance(t, str) else ' | '.join(t) for t in targets]
    event_groups = pd.DataFrame({"event": targets, "event_group": event_group})
    groups = (
        data
            .groupby('session_id')
            [['event']]
            .transform(lambda d: group in d.values)
            .rename(columns={'event': 'has_converted'})
    )
    df =  (
      data
        .merge(event_groups.explode('event'), how='inner')
        .merge(groups, left_index=True, right_index=True)
        # .groupby(['event_group', 'has_converted'])
        .groupby(['event_group'])
        .agg(nb_sessions = ('session_id', 'nunique'))
        .sort_values(by='nb_sessions', ascending=False)
        .reset_index()
    )
    df0 = pd.DataFrame([{'event_group': 'session', 'nb_sessions': data.session_id.nunique()}])
    df = df0.append(df)
    fig = go.Figure(go.Funnel(
        x = df.nb_sessions,
        y = df.event_group,
        textposition = "inside",
        textinfo = "value+percent initial",
    ))
    fig.update_layout(
        height=500,
        title='Conversion Funnel'
    )
    return fig
