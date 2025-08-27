# ================================================================================
# DT Plot Functions Utility Module
# ================================================================================

# Key Points for Building Your Own Plotting Functions:
# --------------------------------------------------------------------------------
# 1. Arguments:
#    - `ev`: Event object containing the relevant DT data.
#    - `wheel`, `sector`, `station`: Integers specifying the DT chamber location.
#    - `ax_phi`, `ax_eta`: Optional matplotlib Axes for phi and eta projections.
#    - `particle_type`: String specifying the type of DT object to plot (e.g., 'digis', 'tps', 'fwshowers').
#    - `cmap_var`: String for the variable to use for color mapping (optional).
#    - `**kwargs`: Additional keyword arguments passed to plotting or patch constructors.

# 2. Return Types:
#    - Each function returns a tuple: (phi_object | None, eta_object | None).
#    - Each element is either: A matplotlib object (e.g., Patch, plot, scatter) or a list of such objects, all supporting the `.remove()` method.


import warnings
from typing import Any, Dict, List, Optional, Tuple

from matplotlib.axes._axes import Axes
from matplotlib.patches import Patch, Circle, Rectangle
from numpy import array, percentile, sqrt
from pandas import DataFrame

from dtpr.base import Event, Particle
from dtpr.utils.functions import color_msg, get_cached_station
from mpldts.geometry import AMDTSegments
from mpldts.patches import DTStationPatch, MultiDTSegmentsPatch

# --------------------------------------- Utility Functions -------------------------------------- #

def test_builder(ev: Event, **kwargs) -> None:
    return None, None

def _validate_axes(ax: Optional[Axes]) -> None:
    """
    Check if the provided axes are valid for plotting.

    Args:
        ax: The axes to check.
        
    Raises:
        ValueError: If the axes are not valid.
    """
    if ax is not None and not isinstance(ax, Axes):
        raise ValueError(f"'ax' should be an instance of matplotlib.axes._axes.Axes class not {type(ax)}")

def get_dt_info(ev: Event, particle_type: str = "digis", **filter_kwargs) -> DataFrame:
    """
    Get DT information from the event based on the particle type and filter criteria.

    :param ev: The event containing DT data.
    :type ev: Event
    :param particle_type: The type of particles to filter (default is "digis").
    :type particle_type: str
    :param filter_kwargs: Additional filtering criteria.
    :return: DataFrame containing the filtered DT information.
    :rtype: pandas.DataFrame
    """
    info = DataFrame([particle.__dict__ for particle in ev.filter_particles(particle_type, **filter_kwargs)])

    if not info.empty:
        # Check if the required columns are present
        if any( not col_name in info.columns for col_name in ["sl", "l", "w"]):
                raise ValueError(f"attributes 'sl', 'l', and 'w' must be present in {particle_type} to be represented in a DT plot")
    return info

def _display_no_data_message(ax: Axes, particle_type: str, description: str) -> None:
    """
    Display a message on the axes when no data is found.
    
    Args:
        ax: Matplotlib axes
        particle_type: Type of particles
        description: Description of the filter (e.g., "wheel 1")
    """
    message = f"No {particle_type} found in {description}"
    color_msg(message, "red")
    ax.text(
        0.5, 0.5, message,
        horizontalalignment='center',
        verticalalignment='center',
        transform=ax.transAxes
    )

def get_shower_segment(shower, version=1, local=False):
    """
    Get the segment representing the shower.
    
    Args:
        shower: Shower object
        version: Version of calculation method (1 or 2)
        local: Whether to return local or global coordinates
        
    Returns:
        Array containing first and last cell centers that represent the shower segment.
    """
    parent_station = get_cached_station(shower.wh, shower.sc, shower.st)
    layer = parent_station.super_layer(shower.sl).layer(2)
    if version == 1: # compute using the wires profile
        # dump profile to wires numbers
        try:
            wires = [wn for wn, nh in enumerate(shower.wires_profile) for _ in range(nh)]
            q75, q25 = map(int, percentile(wires, [75, 25]))
            # use only the wires in the range [q25, q75]
            wires = sorted([wire for wire in wires if wire >= q25 and wire <= q75 ])
            wires[-1] = wires[-1] + 1 if wires[-1] == wires[0] else wires[-1]

            first_wire = wires[0] if wires[0] >= layer._first_cell_id else layer._first_cell_id
            last_wire = wires[-1] if wires[-1] <= layer._last_cell_id else layer._last_cell_id
            first_shower_cell = layer.cell(first_wire)
            last_shower_cell = layer.cell(last_wire)
        except:
            first_shower_cell = layer.cell(shower.min_wire)
            last_shower_cell = layer.cell(shower.max_wire)

    elif version == 2: # compute using max and min wire numbers
        first_shower_cell = layer.cell(shower.min_wire)
        last_shower_cell = layer.cell(shower.max_wire)
    else:
        raise ValueError(f"Unsupported version: {version}")

    if local:
        return array([first_shower_cell.local_center, last_shower_cell.local_center]) # a, b local
    else:
        return array([first_shower_cell.global_center, last_shower_cell.global_center]) # a, b global

# --------------------------------------------------------------------------------------------------

def map_seg_attrs(particle: Particle, particle_type="tps") -> Dict[str, Any]:
    """
    Map the attributes of segment like particle to the format requirted by mpldts.geometry.AMDTSegments.
    
    Args:
        particle: Particle object
        particle_type: Type of particle
        
    Returns:
        Dictionary containing segment attributes
        
    Raises:
        ValueError: If required attributes are missing or particle type is unknown.
    """
    info = {}
    if particle_type == "tps":
        info.update({
            "parent": get_cached_station(particle.wh, particle.sc, particle.st),
            "index": getattr(particle, "index", -1),
            "sl": getattr(particle, "sl", None),
            "angle": -1 * getattr(particle, "dirLoc_phi", None),
            "position": getattr(particle, "posLoc_x", None),
        })
    elif particle_type == "segments":
        warnings.warn("'segments' particle type is not yet implemented")
    else:
        raise ValueError(f"Unknown particle type: {particle_type}")

    for key, val in info.items():
        if val is None:
            raise ValueError(f"attribute {key} must be present in {particle_type} to be represented in a DT plot")

    return info

def embed_dt2axes(ev, 
    wheel: int, 
    sector: int, 
    station: int, 
    ax_phi: Optional[Axes] = None, 
    ax_eta: Optional[Axes] = None, 
    particle_type: Optional[str] = 'digis', 
    cmap_var: Optional[str] = 'time', 
    **kwargs
) -> Tuple[Optional[DTStationPatch], Optional[DTStationPatch]]:
    """
    Embed DT chamber data into local phi and/or eta axes.
    
    Args:
        ev: Event containing DT data
        wheel: Wheel identifier
        sector: Sector identifier  
        station: Station identifier
        ax_phi: Matplotlib axes for phi view
        ax_eta: Matplotlib axes for eta view
        particle_type: Type of particles to plot
        cmap_var: Variable for color mapping
        **kwargs: Additional arguments for patches
        
    Returns:
        Tuple of (phi_patch, eta_patch)
        
    Raises:
        ValueError: If neither ax_phi nor ax_eta is provided or axes are invalid.
    """
    if ax_phi == None and ax_eta == None:
        raise ValueError("At least one 'ax_phi' or 'ax_eta' should be provided.")
    _validate_axes(ax_phi)
    _validate_axes(ax_eta)

    dt_info = get_dt_info(ev, particle_type=particle_type, wh=wheel, sc=sector, st=station)
    if dt_info.empty:
        _dti = None
    else:
        if cmap_var not in dt_info.columns:
            raise ValueError(f"{cmap_var} must be present in {particle_type} data")
        _dti = dt_info[["sl", "l", "w", cmap_var]]

    _dt_chamber = get_cached_station(wheel, sector, station, dt_info=_dti)
    phi_patch, eta_patch = None, None

    if ax_phi is not None:
        phi_patch = DTStationPatch(
            _dt_chamber,
            ax_phi,
            local=True,
            faceview="phi",
            vmap=cmap_var,
            **kwargs    
        )
    if ax_eta is not None:
        eta_patch = DTStationPatch(
            _dt_chamber,
            ax_eta,
            local=True,
            faceview="eta",
            vmap=cmap_var,
            **kwargs
        )

    return phi_patch, eta_patch

def embed_dts2axes(
    ev: Event, 
    wheel: Optional[int] = None, 
    sector: Optional[int] = None, 
    ax_phi: Optional[Axes] = None,
    ax_eta: Optional[Axes] = None,
    particle_type: Optional[str] = 'digis', 
    cmap_var: Optional[str] = 'time', 
    **kwargs
) -> Tuple[Optional[List[DTStationPatch]], Optional[List[DTStationPatch]]]:
    """
    Embed DT chambers data into global phi or eta axes.
    
    Args:
        ev: Event containing DT data
        wheel: Wheel identifier (exclusive with sector)
        sector: Sector identifier (exclusive with wheel)
        ax: Matplotlib axes
        particle_type: Type of particles to plot
        cmap_var: Variable for color mapping
        **kwargs: Additional arguments for patches
        
    Returns:
        List of DT station patches or None if no data
        
    Raises:
        ValueError: If validation fails.
    """
    _validate_axes(ax_phi)
    _validate_axes(ax_eta)

    def _aux_f(ax, faceview, dt_info):
        if not dt_info.empty and cmap_var not in dt_info.columns:
            raise ValueError(f"{cmap_var} must be present in {particle_type} data")

        if dt_info.empty:
            description = f'wheel {wheel}' if faceview == 'phi' else f'sector {sector}'
            _display_no_data_message(ax, particle_type, description)
            return None

        patches = []
        for (wh, sc, st), dt_info in dt_info.groupby(["wh", "sc", "st"]):
                _dti = dt_info[["sl", "l", "w", cmap_var]]
                _dt_chamber = get_cached_station(wh, sc, st, dt_info=_dti)
                if _dt_chamber is None:
                    continue
                patches.append(
                    DTStationPatch(
                        station=_dt_chamber,
                        axes=ax,
                        local=False,
                        faceview=faceview,
                        vmap=cmap_var,
                        **kwargs
                    )
                )

        return patches

    phi_patches, eta_patches = None, None

    if ax_phi is not None:
        if wheel is None:
            raise ValueError("Wheel must be specified when using ax_phi.")
        dt_info = get_dt_info(ev, particle_type=particle_type, wh=wheel)
        phi_patches = _aux_f(ax_phi, "phi", dt_info)

    if ax_eta is not None:
        if sector is None:
            raise ValueError("Sector must be specified when using ax_eta.")
        dt_info = get_dt_info(ev, particle_type=particle_type, sc=sector)
        eta_patches = _aux_f(ax_eta, "eta", dt_info)

    return phi_patches, eta_patches

def embed_segs2axes_glob(
    ev : Event, 
    wheel: Optional[int] = None, 
    sector: Optional[int] = None, 
    ax_phi: Optional[Axes] = None,
    ax_eta: Optional[Axes] = None,
    particle_type: Optional[str] = 'tps', 
    cmap_var: Optional[str] = 'quality', 
    **kwargs
) -> Tuple[Optional[Dict], Optional[Dict]]:
    """
    Embed segments patches into global phi or eta axes.

    Args:
        ev: Event containing segment data
        wheel: Wheel identifier (exclusive with sector)
        sector: Sector identifier (exclusive with wheel)
        ax: Matplotlib axes
        particle_type: Type of particles to plot
        cmap_var: Variable for color mapping
        **kwargs: Additional arguments for patches
        
    Returns:
        List of segment patches or None if no data
    """
    _validate_axes(ax_phi)
    _validate_axes(ax_eta)

    def _aux_f(ax, faceview, particles):
        if not particles:
            return None
        segs_info = [map_seg_attrs(part, particle_type=particle_type) for part in particles]
        if not segs_info:
            return None
        am_segs = AMDTSegments(segs_info)
        seg_patches = MultiDTSegmentsPatch(
            segments=am_segs, 
            axes=ax, 
            local=False, 
            faceview=faceview, 
            vmap=cmap_var, 
            **kwargs
        ).patches

        return seg_patches

    phi_patches, eta_patches = None, None

    if ax_phi is not None:
        if wheel is None:
            raise ValueError("Wheel must be specified when using ax_phi.")
        particles = ev.filter_particles(particle_type, wh=wheel)
        phi_patches = _aux_f(ax_phi, "phi", particles)
    if ax_eta is not None:
        if sector is None:
            raise ValueError("Sector must be specified when using ax_eta.")
        particles = ev.filter_particles(particle_type, sc=sector)
        eta_patches = _aux_f(ax_eta, "eta", particles)

    return phi_patches, eta_patches

def embed_segs2axes_loc(
    ev: Event, 
    wheel: int, 
    sector: int, 
    station: int, 
    ax_phi: Optional[Axes] = None, 
    ax_eta: Optional[Axes] = None, 
    particle_type: Optional[str] = 'tps', 
    cmap_var: Optional[str] = 'quality', 
    **kwargs
) -> Tuple[Optional[Dict], Optional[Dict]]:
    """
    Embed segments patches into local phi and/or eta axes.

    Args:
        ev: Event containing segment data
        wheel: Wheel identifier
        sector: Sector identifier
        station: Station identifier
        ax_phi: Matplotlib axes for phi view
        ax_eta: Matplotlib axes for eta view
        particle_type: Type of particles to plot
        cmap_var: Variable for color mapping
        **kwargs: Additional arguments for patches
        
    Returns:
        Tuple of (phi_patches, eta_patches)
    """
    _validate_axes(ax_phi)
    _validate_axes(ax_eta)

    if ax_phi is None and ax_eta is None:
        raise ValueError("At least one of 'ax_phi' or 'ax_eta' must be provided.")

    particles = ev.filter_particles(particle_type, wh=wheel, sc=sector, st=station)
    if not particles:
        return None, None

    segs_info = [map_seg_attrs(part, particle_type=particle_type) for part in particles]
    if not segs_info:
        return None, None

    am_segs = AMDTSegments(segs_info)
    phi_patch, eta_patch = None, None
    
    if ax_phi is not None:
        phi_patch = MultiDTSegmentsPatch(
            segments=am_segs, 
            axes=ax_phi, 
            local=True, 
            faceview="phi", 
            vmap=cmap_var, 
            **kwargs
        ).patches
        
    if ax_eta is not None:
        eta_patch = MultiDTSegmentsPatch(
            segments=am_segs, 
            axes=ax_eta, 
            local=True, 
            faceview="eta", 
            vmap=cmap_var, 
            **kwargs
        ).patches

    return phi_patch, eta_patch

def embed_simhits2axes_loc(
    ev: Event, 
    wheel: int, 
    sector: int, 
    station: int, 
    ax_phi: Optional[Axes] = None, 
    ax_eta: Optional[Axes] = None, 
    particle_type: Optional[str] = 'simhits', 
    **kwargs
) -> Tuple[Optional[List], Optional[List]]:
    """
    Embed simHits data into local phi and/or eta axes as scatter points.
    
    Args:
        ev: Event containing simhit data
        wheel: Wheel identifier
        sector: Sector identifier
        station: Station identifier
        ax_phi: Matplotlib axes for phi view
        ax_eta: Matplotlib axes for eta view
        particle_type: Type of particles to plot
        **kwargs: Additional arguments for scatter plots
        
    Returns:
        List of scatter plot collections
    """
    _validate_axes(ax_phi)
    _validate_axes(ax_eta)

    if ax_phi is None and ax_eta is None:
        raise ValueError("At least one of 'ax_phi' or 'ax_eta' must be provided.")

    particles = ev.filter_particles(particle_type, wh=wheel, sc=sector, st=station)
    if not particles:
        return None, None

    style_map = {
        13: {"color": "red", "s": 35, "marker": "*"},
        11: {"color": "yellow", "s": 10, "marker": "o"},
        22: {"color": "blue", "s": 20, "marker": "s"},
    }

    _parent_station = get_cached_station(wheel, sector, station)

    patch_phi, patch_eta = None, None

    for part in particles:
        if not hasattr(part, "sl") or not hasattr(part, "l") or not hasattr(part, "w"):
            raise ValueError(f"Particle {part} does not have required attributes 'sl', 'l', and 'w' for {particle_type}")
        sl, l, w, particle_type = part.sl, part.l, part.w, abs(part.particle_type)
        center = _parent_station.super_layer(sl).layer(l).cell(w).local_center
        if sl==2:
            if patch_eta is None:
                patch_eta = []
            patch_eta.append(ax_eta.scatter(center[0], center[2], **style_map[particle_type], **kwargs))
        else:
            if patch_phi is None:
                patch_phi = []
            patch_phi.append(ax_phi.scatter(center[0], center[2], **style_map[particle_type], **kwargs))

    return patch_phi, patch_eta

def embed_shower2axes_loc(ev, 
    wheel: int, 
    sector: int, 
    station: int, 
    ax_phi: Optional[Axes] = None, 
    ax_eta: Optional[Axes] = None, 
    particle_type: Optional[str] = 'fwshowers', 
    **kwargs
) -> Tuple[Optional[List], Optional[List]]:
    """
    Embed shower patches into local phi and/or eta axes.
    
    Args:
        ev: Event containing shower data
        wheel: Wheel identifier
        sector: Sector identifier
        station: Station identifier
        ax_phi: Matplotlib axes for phi view
        ax_eta: Matplotlib axes for eta view
        particle_type: Type of particles to plot
        **kwargs: Additional arguments for plot
        
    Returns:
        List of plot objects
    """
    _validate_axes(ax_phi)
    _validate_axes(ax_eta)
    if ax_phi is None and ax_eta is None:
        raise ValueError("At least one of 'ax_phi' or 'ax_eta' must be provided.")

    particles = ev.filter_particles(particle_type, wh=wheel, sc=sector, st=station)
    if not particles:
        return None, None

    patch_phi, patch_eta = None, None

    for part in particles:
        if any([not hasattr(part, attr) for attr in ["sl", "min_wire", "max_wire"]]):
            raise ValueError(f"Particle {particle_type} does not have any (or all) required attributes 'sl', 'min_wire', and 'max_wire'")
        
        segment = get_shower_segment(part, version=2, local=True)
        if ax_phi is not None and part.sl != 2:
            if patch_phi is None:
                patch_phi = []
            patch_phi.extend(ax_phi.plot(segment[:, 0], segment[:, 2], **kwargs))
        if ax_eta is not None and part.sl == 2:
            if patch_eta is None:
                patch_eta = []
            patch_eta.extend(ax_eta.plot(segment[:, 0], segment[:, 2], **kwargs))

    return patch_phi, patch_eta

def embed_shower2axes_glob(
    ev: Event, 
    wheel: Optional[int] = None, 
    sector: Optional[int] = None, 
    ax_phi: Optional[Axes] = None,
    ax_eta: Optional[Axes] = None,
    particle_type: str = 'fwshowers', 
    **kwargs
) -> Tuple[Optional[List], Optional[List]]:
    """
    Embed shower patches into global phi or eta axes.
    
    Args:
        ev: Event containing shower data
        wheel: Wheel identifier (exclusive with sector)
        sector: Sector identifier (exclusive with wheel)
        ax: Matplotlib axes
        particle_type: Type of particles to plot
        **kwargs: Additional arguments for plot
        
    Returns:
        List of plot objects
    """
    _validate_axes(ax_phi)
    _validate_axes(ax_eta)

    def _aux_f(ax, faceview, particles):
        if not particles:
            return None
        patches = []
        for part in particles:
            if any([not hasattr(part, attr) for attr in ["sl", "min_wire", "max_wire"]]):
                raise ValueError(f"Particle {particle_type} does not have any (or all) required attributes 'sl', 'min_wire', and 'max_wire'")
            segment = get_shower_segment(part, version=2, local=False)
            if faceview == "phi":
                patches.extend(ax.plot(segment[:, 0], segment[:, 1], **kwargs))
            else:
                patches.extend(ax.plot(segment[:, 2], sqrt(segment[:, 0]**2 + segment[:, 1]**2), **kwargs))
        return patches

    phi_patches, eta_patches = None, None

    if ax_phi is not None:
        if wheel is None:
            raise ValueError("Wheel must be specified when using ax_phi.")
        particles = ev.filter_particles(particle_type, wh=wheel)
        phi_patches = _aux_f(ax_phi, "phi", particles)
    if ax_eta is not None:
        if sector is None:
            raise ValueError("Sector must be specified when using ax_eta.")
        particles = ev.filter_particles(particle_type, sc=sector)
        eta_patches = _aux_f(ax_eta, "eta", particles)

    return phi_patches, eta_patches

def embed_cms_global_shadow(
    ev: Event,
    wheel: Optional[int] = None, 
    sector: Optional[int] = None, 
    ax_phi: Optional[Axes] = None,
    ax_eta: Optional[Axes] = None,
    **kwargs
) -> Tuple[Optional[Patch], Optional[Patch]]:
    _validate_axes(ax_phi)
    _validate_axes(ax_eta)

    patch_phi, patch_eta = None, None

    if ax_phi is not None:
        patch_phi = Circle(
            xy= (0, 0),  # Center of the circle
            radius= 800,
            **kwargs,
        )
        ax_phi.add_patch(patch_phi)
    if ax_eta is not None:
        patch_eta = Rectangle(
            xy= (-700, 0),  # Bottom-left corner of the rectangle
            width= 1400,  # Width of the rectangle
            height= 800,  # Height of the rectangle
            **kwargs
        )
        ax_eta.add_patch(patch_eta)

    return patch_phi, patch_eta
