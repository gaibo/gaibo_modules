import matplotlib.pyplot as plt
import os
from pathlib import Path

BIG_FIGSIZE = (19.2, 10.8)


def suplabel(x_or_y, label_text, fig=None, y_right=False, label_pad=5, ha='center', va='center', **label_kwargs):
    """ Add "super" xlabel or ylabel to figure, in the style of fig.suptitle(),
        to span multiple graphical axes
    :param x_or_y: 'x'- or 'y'-axis, on which to add text label
    :param label_text: the text label to add
    :param fig: figure on which to add; set None to use current figure via plt.gcf()
    :param y_right: set True if 'y'-axis and label should be on right side, not default left
    :param label_pad: "padding" from the axis; default 5
    :param ha: horizontal alignment; 'center' | 'right' | 'left'
    :param va: vertical alignment; 'center' | 'top' | 'bottom' | 'baseline'
    :param label_kwargs: additional kwargs for coniguring label_text
    :return: Text object added to fig
    """
    if fig is None:
        fig = plt.gcf()
    x_or_y = x_or_y.lower()
    if x_or_y == 'x':
        x = 0.5
        ymins = [ax.get_position().ymin for ax in fig.axes]
        ymin = min(ymins)
        y = ymin - label_pad/fig.dpi
        rotation = 0
    elif x_or_y == 'y':
        if y_right:
            xmaxs = [ax.get_position().xmax for ax in fig.axes]
            xmax = min(xmaxs)
            x = xmax + label_pad/2/fig.dpi  # Empirically, y-axis label_pad looks better smaller
        else:
            xmins = [ax.get_position().xmin for ax in fig.axes]
            xmin = min(xmins)
            x = xmin - label_pad/2/fig.dpi  # Empirically, y-axis label_pad looks better smaller
        y = 0.5
        rotation = 90
    else:
        raise ValueError(f"x_or_y must specify 'x' or 'y' axis; '{x_or_y}' is unclear")
    return fig.text(x, y, label_text, rotation=rotation, ha=ha, va=va,
                    transform=fig.transFigure, **label_kwargs)


def save_fig(figure, save_name, save_dir='.', bbox_inches='tight', **savefig_kwargs):
    """ Export figure to specified directory
    :param figure: figure handle generated by matplotlib (e.g. fig, ax = plt.subplots())
    :param save_name: file name to save figure as (usually string ending in .png)
    :param save_dir: file directory to save to (can be string or pathlib.Path)
    :param bbox_inches: fig.savefig() argument; set 'tight' to crop borders, set None for default
    :param savefig_kwargs: additional optional arguments for fig.savefig()
    :return: None
    """
    if not isinstance(save_dir, Path):
        save_dir = Path(save_dir)   # Converts string path cleanly
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
        print(f'save_fig: made directory {save_dir}')
    # Write figure
    save_loc = save_dir / save_name
    figure.savefig(save_loc, bbox_inches=bbox_inches, **savefig_kwargs)
    print(f'save_fig: wrote {save_loc}')
