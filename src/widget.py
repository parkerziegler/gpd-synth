import threading
import ipywidgets as widgets
import base64
from IPython.display import Javascript, display
from pandas import DataFrame
from synthesize import lazy_synthesize_gen

def synth_thread(gdfs: dict[str, DataFrame], target: DataFrame, out: widgets.Output):
  gen = lazy_synthesize_gen(gdfs, target)
  html = widgets.HTML()
  out.append_display_data(html)
  out.append_display_data(Javascript("""
    window.createCodeCell = function(encoded) {
      var code = IPython.notebook.insert_cell_below('code');
      code.set_text(atob(encoded));
    }
  """))

  for [count, candidate, found] in gen:
    html.value = f"<b>Synthesizing...</b> candidates generated: {count}"
  if found:
    encoded = (base64.b64encode(str.encode(repr(candidate)))).decode()
    html.value = f"""
      <b>Program found!</b>
      <pre>{candidate}</pre>
      <button class="lm-Widget p-Widget jupyter-widgets jupyter-button widget-button" onclick="createCodeCell('{encoded}')">
        Add to notebook
      </button>"""
    
  else:
    html.value = f"No program was found."

def synthesize(gdfs: dict[str, DataFrame], target: DataFrame) -> widgets.Output:
  out = widgets.Output()
  thread = threading.Thread(
    target=synth_thread,
    args=(gdfs, target, out))
  thread.start()
  return out
