import matplotlib.pyplot as plt
from mplhep import style
from mpldts.patches import DTRelatedPatch
from ..functions import parse_plot_configs
from typing import Any, Dict, List, Optional


class ArtistManager:
    """
    Utility class to manage embedding and deleting matplotlib artists (patches) on axes.
    Tracks included artists for both phi and eta views, and provides methods for embedding and deletion.

    Args:
        artist_builders (Optional[dict]): Mapping of artist names to builder functions.
        ax_phi (Optional[matplotlib.axes.Axes]): Axes for the phi view.
        ax_eta (Optional[matplotlib.axes.Axes]): Axes for the eta view.
    """

    def __init__(
        self,
        artist_builders: Optional[Dict[str, Any]] = None,
        ax_phi: Optional[Any] = None,
        ax_eta: Optional[Any] = None,
    ):
        self.artists_included = {"phi": {}, "eta": {}}
        self.ax_phi = ax_phi
        self.ax_eta = ax_eta

        if artist_builders is None:
            # Parse plot configurations
            mplhep_style, figure_configs, self.artist_builders = parse_plot_configs().values()
            if mplhep_style:
                plt.style.use(getattr(style, mplhep_style))
            if figure_configs:
                plt.rcParams.update(figure_configs)
            if not self.artist_builders:
                raise ValueError("No artist builders found in the configuration file.")
        else:
            self.artist_builders = artist_builders

    def embed_artists(
        self,
        artist_names: List[str],
        builder_kwargs: Dict[str, Any],
        faceview: Optional[str] = None,
    ) -> None:
        """
        Embed artists on the phi and eta axes.

        Args:
            artist_names (List[str]): List of artist names to include.
            builder_kwargs (Dict[str, Any]): Arguments to pass to the builder functions.
        """
        # Ensure correct axes are passed
        builder_kwargs["ax_phi"] = self.ax_phi if faceview is None or faceview == "phi" else None
        builder_kwargs["ax_eta"] = self.ax_eta if faceview is None or faceview == "eta" else None
        for artist_name in artist_names:
            artist_builder = self.artist_builders.get(artist_name)
            if artist_builder is None:
                raise ValueError(f"Artist '{artist_name}' not found in the configuration file.")
            if all(artist_name in self.artists_included[view] for view in ["phi", "eta"]):
                continue  # Skip if already embedded

            patches_phi, patches_eta = artist_builder(**builder_kwargs)

            self._add_patches_to_included_list(patches_phi, "phi", artist_name)
            self._add_patches_to_included_list(patches_eta, "eta", artist_name)

        self.refresh_axes(self.ax_phi)
        self.refresh_axes(self.ax_eta)

    def delete_artists(self, artist_names: List[str]) -> None:
        """
        Delete specified artists from the phi and eta axes.

        Args:
            artist_names (List[str]): List of artist names to delete.
        """

        def _remove_artist(artist):
            removed = False
            if isinstance(artist, DTRelatedPatch):
                for collection in getattr(artist, "_collections", []):
                    collection.remove()
                removed = True
            elif hasattr(artist, "remove"):
                artist.remove()
                removed = True
            else:
                raise ValueError(
                    f"Artist {artist} does not have a remove method or is not a DTRelatedPatch."
                )
            return removed

        for artist_name in artist_names:
            if all(artist_name not in self.artists_included[view] for view in ["phi", "eta"]):
                continue  # Skip if not included

            patches_phi = self.artists_included["phi"].get(artist_name, [])
            patches_eta = self.artists_included["eta"].get(artist_name, [])
            patches = []
            if patches_phi:
                patches.extend(patches_phi)
            if patches_eta:
                patches.extend(patches_eta)

            removed_flags = list(map(_remove_artist, patches))
            if all(removed_flags):
                self.artists_included["phi"].pop(artist_name, None)
                self.artists_included["eta"].pop(artist_name, None)
            else:
                raise ValueError(f"Failed to remove all artists for '{artist_name}'.")

        self.refresh_axes(self.ax_phi)
        self.refresh_axes(self.ax_eta)

    def refresh_axes(self, axes: Optional[Any]) -> None:
        """
        Refresh the given axes by autoscaling and redrawing.

        Args:
            axes (matplotlib.axes.Axes or None): The axes to refresh.
        """
        if axes is not None:
            axes.autoscale()
            axes.set_aspect("equal", adjustable="datalim")
            axes.figure.canvas.draw()

    def _add_patches_to_included_list(self, patches: Any, faceview: str, artist_name: str) -> None:
        """
        Add patches to the internal included list for a given view.

        Args:
            patches: The patch or list/dict of patches to add.
            faceview (str): "phi" or "eta".
            artist_name (str): The name of the artist.
        """
        if patches is None:
            return
        elif isinstance(patches, dict):
            _patches = [val for _, val in patches.items()]
        elif not isinstance(patches, list):
            _patches = [patches]
        else:
            _patches = patches

        self.artists_included[faceview][artist_name] = _patches
